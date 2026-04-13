"""
企业微信 Webhook 告警推送
只推拉盘评估结果，精简格式
"""
import logging
from datetime import datetime, timezone, timedelta
import httpx
import config

logger = logging.getLogger(__name__)

CST = timezone(timedelta(hours=8))


def _fmt_money(v: float) -> str:
    if v >= 1e9:
        return f"${v/1e9:.1f}B"
    if v >= 1e6:
        return f"${v/1e6:.1f}M"
    if v >= 1e3:
        return f"${v/1e3:.0f}K"
    return f"${v:.0f}"


def _format_alert(alert: dict) -> str:
    """精简格式：一眼能做决策"""
    t = alert.get("type", "")
    now = datetime.now(CST).strftime("%H:%M")

    if t == "拉盘监控报告":
        lines = [f"📋 Alpha Hunter ({alert['count']}个) {now}"]
        for item in alert.get("items", []):
            lines.append("")
            sym = item["symbol"]
            sc = item["score"]
            level = "🟢" if sc >= 70 else "🟡" if sc >= 50 else "🟠"
            lines.append(f"{level} {sym} {sc:.0f}分 ${item['price']:.4f}")
            lines.append(f"庄成本: {item['mm_cost']}")
            if item["cost_per_pct"] > 0:
                lines.append(f"每涨1%: {_fmt_money(item['cost_per_pct'])}")
            if item["short_val"] > 0:
                lines.append(f"空头: {_fmt_money(item['short_val'])} ({item['short_pct']:.0%})")
            lines.append(f"预估空间: {item['pump_est']}")
            if item.get("mm_controlled"):
                lines.append("⚠️ MM控盘")
            lines.append(f"24h: {item['price_chg']:+.1f}%")
            lines.append(item["advice"])
        return "\n".join(lines)

    if t != "拉盘评估":
        return ""

    # === 拉盘评估：精简格式 ===
    score = alert.get("score", 0)
    sym = alert["symbol"]
    sl = alert.get("short_liq", {})
    kc = alert.get("kline_cost", {})
    conc = alert.get("concentration", {})
    lpr = alert.get("liq_profit_ratio", 0)
    price_chg = alert.get("price_change_24h", 0)

    # 第一行：币种 + 评分 + 继续拉概率
    if lpr > 2:
        prob = "极高"
    elif lpr > 1:
        prob = "高"
    elif lpr > 0.5:
        prob = "中"
    elif sl.get("short_ratio", 0) > 0.6:
        prob = "中(空头肥)"
    else:
        prob = "低"

    level = "🟢" if score >= 70 else "🟡" if score >= 50 else "🟠"
    header = f"{level} {sym} {score:.0f}分 | 继续拉: {prob}"

    # 启动信号
    if alert.get("_is_launch"):
        header = f"🚀 {sym} {score:.0f}分 | 启动信号!"

    lines = [header]

    # 预估空间
    short_val = sl.get("short_value", 0)
    short_pct = sl.get("short_ratio", 0)
    if short_val > 0:
        # 找最大可盈利的拉盘幅度
        liq_map = sl.get("liquidation_map", {})
        best_target = ""
        for label in ["+50%", "+33%", "+20%", "+10%", "+5%"]:
            if label in liq_map and liq_map[label]["cumulative_liquidation"] > 0:
                best_target = label
                break
        lines.append(f"空头 {_fmt_money(short_val)} ({short_pct:.0%}) → 拉到{best_target}全爆")

    # 清算收益 vs 成本
    if lpr > 0:
        liq_20 = sl.get("liquidation_map", {}).get("+20%", {}).get("cumulative_liquidation", 0)
        cost_per = kc.get("cost_per_pct", 0)
        if cost_per > 0:
            cost_20 = cost_per * 20
            lines.append(f"拉+20%: 成本{_fmt_money(cost_20)} → 清算{_fmt_money(liq_20)} ({lpr:.1f}x)")

    # 每涨1%成本 + 历史最大涨幅
    if kc.get("cost_per_pct", 0) > 0:
        lines.append(f"每涨1%: {_fmt_money(kc['cost_per_pct'])} | 历史最大: {kc['max_pump_pct']:.0%}")

    # MM控盘
    if conc.get("is_mm_controlled"):
        lines.append(f"MM控盘 (盘口仅覆盖{conc['spread_pct']:.1%})")

    # 当前状态
    vol = alert.get("quote_volume_24h", 0)
    lines.append(f"24h: {price_chg:+.1f}% | 量{_fmt_money(vol)} | {now}")

    return "\n".join(lines)


async def send_alert(alerts: list[dict]):
    """发送告警到企业微信，每条单独发"""
    if not alerts:
        return
    if not config.WECOM_WEBHOOK_URL:
        for a in alerts:
            msg = _format_alert(a)
            if msg:
                logger.warning("[未配置Webhook] %s", msg)
        return

    for a in alerts:
        text = _format_alert(a)
        if not text:
            continue
        payload = {"msgtype": "text", "text": {"content": text}}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(config.WECOM_WEBHOOK_URL, json=payload)
                resp.raise_for_status()
                result = resp.json()
                if result.get("errcode") != 0:
                    logger.error("推送失败: %s", result)
                else:
                    logger.info("推送: %s", a.get("symbol", ""))
        except Exception as e:
            logger.error("推送异常: %s", e)

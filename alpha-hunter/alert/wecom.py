"""
企业微信 Webhook 告警推送
"""
import logging
from datetime import datetime, timezone, timedelta
import httpx
import config

logger = logging.getLogger(__name__)

CST = timezone(timedelta(hours=8))

EMOJI_MAP = {
    "OI异动": "📊",
    "资金费率异动": "💰",
    "成交量突增": "📈",
    "价格波动": "⚡",
    "大额清算": "🔥",
    "实时清算": "💥",
    "多空比异动": "⚖️",
    "空头挤压": "🔥",
    "多头挤压": "💀",
    "阶段判断": "🎯",
    "涨幅榜异动": "🔺",
    "挤压前兆": "⚡",
    "拉盘评估": "🎰",
    "拉盘监控报告": "📋",
}


def _format_alert(alert: dict) -> str:
    """将单条告警格式化为可读文本"""
    emoji = EMOJI_MAP.get(alert["type"], "🚨")
    now = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"{emoji} {alert['type']} — {alert['symbol']}"]

    # 启动信号标记
    if alert.get("_is_launch"):
        lines.insert(0, "🚀🚀🚀 启动信号 🚀🚀🚀")
        if alert.get("_launch_signal"):
            lines.append(f"触发: {alert['_launch_signal']}")

    t = alert["type"]
    if t == "拉盘监控报告":
        lines.append(f"观望列表: {alert['count']} 个币种")
        for item in alert.get("top_list", []):
            lines.append(f"  {item}")
        lines.append(f"时间: {now}")
        return "\n".join(lines)
    elif t == "OI异动":
        lines.append(f"方向: {alert['direction']}")
        lines.append(f"OI: {alert['prev_oi']:.2f} → {alert['current_oi']:.2f} ({alert['change_rate']:+.1%})")
        if alert.get("trend_1h"):
            lines.append(f"趋势: {alert['trend_1h']}")
    elif t == "资金费率异动":
        lines.append(f"当前费率: {alert['funding_rate']*100:.4f}%")
        if alert.get("prev_rate") is not None:
            lines.append(f"上次费率: {alert['prev_rate']*100:.4f}%")
        lines.append(f"原因: {', '.join(alert.get('reasons', []))}")
    elif t == "成交量突增":
        lines.append(f"当前5min量: ${alert['current_volume']:,.0f}")
        lines.append(f"1h均值: ${alert['avg_volume']:,.0f} ({alert['ratio']:.1f}x)")
    elif t == "价格波动":
        lines.append(f"价格: {alert['prev_price']:.6f} → {alert['current_price']:.6f} ({alert['change_rate']:+.2%})")
    elif t in ("大额清算", "实时清算"):
        lines.append(f"方向: {alert['side']}")
        lines.append(f"金额: ${alert['value']:,.0f}")
        lines.append(f"价格: {alert['price']:.4f}")
    elif t == "多空比异动":
        lines.append(f"当前多空比: {alert['long_short_ratio']:.2f}")
        if alert.get("prev_ratio") is not None:
            lines.append(f"上次多空比: {alert['prev_ratio']:.2f}")
        lines.append(f"原因: {', '.join(alert.get('reasons', []))}")
    elif t in ("空头挤压", "多头挤压"):
        lines.append(f"描述: {alert['desc']}")
        lines.append(f"OI累积: {alert['accumulation_rate']:+.0%}")
        lines.append(f"OI急降: {alert['oi_drop']:+.0%} (1h)")
        lines.append(f"价格变动: {alert['price_change']:+.0%}")
    elif "OI-价格联动" in t:
        lines.append(f"模式: {alert['pattern']}")
        lines.append(f"描述: {alert['desc']}")
        lines.append(f"OI变化: {alert['oi_change']:+.1%} | 价格变化: {alert['price_change']:+.1%}")
    elif t == "阶段判断":
        lines.append(f"阶段: {alert['emoji']} {alert['phase']}")
        lines.append(f"描述: {alert['desc']}")
        lines.append(f"置信度: {alert['confidence']:.0%}")
        lines.append(f"建议: {alert['suggestion']}")
        lines.append(f"4h数据: OI {alert['oi_change_4h']:+.1%} | Price {alert['price_change_4h']:+.1%}")
    elif t == "涨幅榜异动":
        lines.append(f"类型: {alert['sub_type']}")
        lines.append(f"24h涨跌: {alert['price_change_pct']:+.1f}%")
        lines.append(f"价格: {alert['price']:.6f}")
        lines.append(f"24h成交额: ${alert['quote_volume']:,.0f}")
    elif t == "挤压前兆":
        lines.append(f"模式: {alert['pattern']}")
        lines.append(f"描述: {alert['desc']}")
    elif t == "拉盘评估":
        lines.append(f"评分: {alert['score']:.0f}/100")
        lines.append(f"建议: {alert['advice']}")
        lines.append(f"风险: {alert['risk']}")
        # MM控盘信号
        conc = alert.get("concentration", {})
        if conc.get("is_mm_controlled"):
            lines.append(f"⚠️ MM控盘: 盘口集中度{conc['score']:.0f}/100 (1000档仅覆盖{conc['spread_pct']:.1%})")
        # 空头清算收益
        sl = alert.get("short_liq", {})
        if sl.get("short_value", 0) > 0:
            lines.append(f"空头持仓: ${sl['short_value']:,.0f} (占比{sl['short_ratio']:.0%}, 多空比{sl['long_short_ratio']:.2f})")
            liq_map = sl.get("liquidation_map", {})
            liq_lines = []
            for label in ["+5%", "+10%", "+20%", "+33%"]:
                if label in liq_map:
                    lv = liq_map[label]["cumulative_liquidation"]
                    if lv > 0:
                        liq_lines.append(f"  拉到{label}: 清算${lv:,.0f}")
            if liq_lines:
                lines.append("空头清算预估:")
                lines.extend(liq_lines)
        lpr = alert.get("liq_profit_ratio", 0)
        if lpr > 0:
            verdict = "🟢 MM必拉" if lpr > 2 else "🟡 有动力" if lpr > 1 else "🟠 一般"
            lines.append(f"清算收益/拉盘成本: {lpr:.1f}x → {verdict}")
        # 拉盘成本明细
        costs = alert.get("pump_costs", {})
        if costs:
            cost_lines = []
            for label in ["+50%", "+100%", "+200%", "+300%"]:
                if label in costs:
                    c = costs[label]["cost"]
                    cost_lines.append(f"  拉到{label}: ${c:,.0f}")
            if cost_lines:
                lines.append("拉盘成本(盘口快照):")
                lines.extend(cost_lines)
        # OI 建仓
        oi = alert.get("oi_accumulation", {})
        if oi.get("estimated_cost", 0) > 0:
            lines.append(f"MM建仓估算: ${oi['estimated_cost']:,.0f} (48h OI {oi['oi_change_pct']:+.1%})")
        if alert.get("estimated_multiplier", 0) > 0:
            lines.append(f"预估拉盘空间: {alert['estimated_multiplier']:.0%}")
        # K线反推真实成本
        kc = alert.get("kline_cost", {})
        if kc.get("cost_per_pct", 0) > 0:
            lines.append(f"历史拉盘数据:")
            lines.append(f"  最大涨幅: {kc['max_pump_pct']:.0%} ({kc['pump_hours']}h)")
            lines.append(f"  总消耗: ${kc['pump_volume']:,.0f}")
            lines.append(f"  每涨1%成本: ${kc['cost_per_pct']:,.0f}")
            # 用历史成本估算继续拉的代价
            for target in [50, 100, 200]:
                est = kc['cost_per_pct'] * target
                lines.append(f"  再拉{target}%需: ${est:,.0f}")
        lines.append(f"24h涨幅: {alert['price_change_24h']:+.1f}%")
        lines.append(f"24h成交额: ${alert['quote_volume_24h']:,.0f}")

    lines.append(f"时间: {now}")
    return "\n".join(lines)


async def send_alert(alerts: list[dict]):
    """发送告警到企业微信"""
    if not alerts:
        return
    if not config.WECOM_WEBHOOK_URL:
        for a in alerts:
            logger.warning("[未配置Webhook] %s", _format_alert(a))
        return

    text = "🚨 Alpha Hunter 异动告警\n\n" + "\n\n---\n\n".join(_format_alert(a) for a in alerts)

    payload = {
        "msgtype": "text",
        "text": {"content": text},
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(config.WECOM_WEBHOOK_URL, json=payload)
            resp.raise_for_status()
            result = resp.json()
            if result.get("errcode") != 0:
                logger.error("企业微信推送失败: %s", result)
            else:
                logger.info("企业微信推送成功，%d 条告警", len(alerts))
    except Exception as e:
        logger.error("企业微信推送异常: %s", e)

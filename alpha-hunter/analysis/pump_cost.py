"""
拉盘成本估算 & 进场评估
核心逻辑：
1. 从 Order Book 卖盘深度算出拉到各价位需要多少资金（静态快照）
2. 从历史 K 线反推「每拉1%需要多少成交量」（动态真实成本）
3. 从 OI 增量估算 MM 已投入的建仓成本
4. 盘口集中度分析（MM控盘信号）
5. 综合评分给出进场建议
"""
import asyncio
import logging
from data import binance_futures as bf
from data.redis_store import get_oi_history
import config

logger = logging.getLogger(__name__)

# 拉盘目标倍数
PUMP_TARGETS = [0.5, 1.0, 2.0, 3.0, 5.0]  # +50%, +100%, +200%, +300%, +500%

# 分析窗口
ACCUMULATION_WINDOW_MS = 48 * 60 * 60 * 1000  # 48h OI 建仓窗口


async def estimate_pump_cost(symbol: str) -> dict | None:
    """
    估算单个币种的拉盘成本和空间。
    返回 None 表示数据不足或不符合条件。
    """
    try:
        # 并行获取 order book + 当前行情 + OI + 多空比
        depth_task = bf.fetch_depth(symbol, limit=500)
        oi_task = bf.fetch_open_interest(symbol)
        ticker_task = bf.fetch_tickers([symbol])
        lsr_task = bf.fetch_global_long_short_ratio(symbol, period="5m", limit=1)

        depth, oi_data, tickers, lsr_data = await asyncio.gather(
            depth_task, oi_task, ticker_task, lsr_task, return_exceptions=True,
        )

        if isinstance(depth, Exception) or isinstance(oi_data, Exception):
            return None

        ticker = tickers[0] if isinstance(tickers, list) and tickers else None
        if not ticker:
            return None

        current_price = ticker["price"]
        if current_price <= 0:
            return None

        # 多空比
        long_short_ratio = 1.0
        if isinstance(lsr_data, list) and lsr_data:
            long_short_ratio = lsr_data[0].get("long_short_ratio", 1.0)

        # --- 1. Order Book 拉盘成本（静态快照）---
        asks = depth["asks"]  # [(price, qty), ...] 价格从低到高
        pump_costs = _calc_pump_costs(current_price, asks, PUMP_TARGETS)

        # --- 2. 盘口集中度（MM控盘信号）---
        concentration = _calc_concentration(current_price, asks)

        # --- 3. K线反推真实拉盘成本 ---
        kline_cost = await _estimate_cost_from_klines(symbol)

        # --- 4. OI 建仓成本估算 ---
        oi_cost = await _estimate_oi_accumulation(symbol, current_price)

        # --- 5. 空头清算收益估算 ---
        oi_value = oi_data["oi"] * current_price  # OI 名义价值
        short_liq = _estimate_short_liquidation(oi_value, long_short_ratio, current_price)

        # --- 6. 卖盘薄度评分 ---
        thin_score = _calc_thin_score(current_price, asks)

        # --- 7. 综合评估 ---
        assessment = _assess_pump_potential(
            symbol, current_price, pump_costs, oi_cost, thin_score, ticker,
            concentration, kline_cost, short_liq,
        )

        return assessment

    except Exception as e:
        logger.warning("拉盘成本估算 %s 失败: %s", symbol, e)
        return None


def _calc_pump_costs(
    current_price: float,
    asks: list[tuple[float, float]],
    targets: list[float],
) -> dict[str, dict]:
    """
    计算拉到各目标价位需要吃掉的卖盘资金。
    返回 {"+50%": {"target_price": x, "cost": y, "depth_qty": z}, ...}
    """
    results = {}
    for pct in targets:
        target_price = current_price * (1 + pct)
        total_cost = 0.0
        total_qty = 0.0
        for ask_price, ask_qty in asks:
            if ask_price > target_price:
                break
            total_cost += ask_price * ask_qty
            total_qty += ask_qty
        label = f"+{int(pct * 100)}%"
        results[label] = {
            "target_price": target_price,
            "cost": total_cost,
            "depth_qty": total_qty,
        }
    return results


def _estimate_short_liquidation(
    oi_value: float,
    long_short_ratio: float,
    current_price: float,
) -> dict:
    """
    估算拉到各价位能挤爆多少空头、产生多少清算收益。

    模型假设：
    - 空头持仓 = OI × 空头占比
    - 空头杠杆分布：20x(20%), 10x(35%), 5x(30%), 3x(15%)
    - 清算线 ≈ 入场价 × (1 + 1/杠杆)
    - 假设空头入场价 ≈ 当前价格（最近建仓的最先被清算）
    - 被清算仓位产生等量市价买单（正反馈）
    """
    # 空头占比
    short_ratio = 1 / (1 + long_short_ratio) if long_short_ratio > 0 else 0.5
    short_value = oi_value * short_ratio

    # 杠杆分布 → 清算价格分布
    # (杠杆, 占比, 清算触发涨幅)
    leverage_dist = [
        (20, 0.20, 0.05),   # 20x → 涨5%清算
        (10, 0.35, 0.10),   # 10x → 涨10%清算
        (5,  0.30, 0.20),   # 5x  → 涨20%清算
        (3,  0.15, 0.33),   # 3x  → 涨33%清算
    ]

    # 拉到各价位的累计清算量
    targets = [0.05, 0.10, 0.20, 0.33, 0.50, 1.0]
    liquidation_map = {}

    for target_pct in targets:
        cumulative_liq = 0.0
        for leverage, weight, liq_threshold in leverage_dist:
            if target_pct >= liq_threshold:
                cumulative_liq += short_value * weight
        # 清算产生的强制买入力量
        forced_buy = cumulative_liq
        # MM 从清算中获得的价格推升收益（简化：清算量/总OI × 涨幅 × MM仓位）
        label = f"+{int(target_pct * 100)}%"
        liquidation_map[label] = {
            "target_pct": target_pct,
            "cumulative_liquidation": cumulative_liq,
            "forced_buy_volume": forced_buy,
        }

    # 总空头价值和清算收益汇总
    total_liq_at_20pct = sum(
        short_value * w for _, w, t in leverage_dist if 0.20 >= t
    )

    # 拉盘动力评估：清算收益 vs 拉盘成本
    return {
        "short_value": short_value,
        "short_ratio": short_ratio,
        "long_short_ratio": long_short_ratio,
        "oi_value": oi_value,
        "liquidation_map": liquidation_map,
        "total_liq_at_20pct": total_liq_at_20pct,
    }


def _calc_concentration(current_price: float, asks: list[tuple[float, float]]) -> dict:
    """
    盘口集中度分析 — MM控盘信号。
    如果1000档只覆盖很小的价格范围，说明MM在密集挂单控盘。
    """
    if not asks:
        return {"spread_pct": 0, "is_mm_controlled": False, "score": 0}

    highest_ask = asks[-1][0]
    spread_pct = (highest_ask - current_price) / current_price if current_price > 0 else 0

    # 1000档只覆盖 <5% 说明盘口极度集中 = MM控盘
    if spread_pct < 0.02:
        score = 95  # 极度控盘
    elif spread_pct < 0.05:
        score = 80
    elif spread_pct < 0.10:
        score = 50
    elif spread_pct < 0.30:
        score = 25
    else:
        score = 10  # 正常分散盘口

    return {
        "spread_pct": spread_pct,
        "is_mm_controlled": spread_pct < 0.05,
        "score": score,
    }


async def _estimate_cost_from_klines(symbol: str) -> dict:
    """
    从历史 K 线反推真实拉盘成本。
    逻辑：找到最近的上涨段，计算「每拉1%消耗多少成交额」。
    这比 order book 快照准得多，因为反映了真实的市场摩擦。
    """
    try:
        # 拉48根1h K线
        klines = await bf.fetch_klines(symbol, interval="1h", limit=48)
        if len(klines) < 5:
            return {"cost_per_pct": 0, "max_pump_pct": 0, "pump_volume": 0, "pump_hours": 0}

        # 找最大的连续上涨段
        best_pump = _find_best_pump_segment(klines)
        if not best_pump:
            return {"cost_per_pct": 0, "max_pump_pct": 0, "pump_volume": 0, "pump_hours": 0}

        return best_pump
    except Exception:
        return {"cost_per_pct": 0, "max_pump_pct": 0, "pump_volume": 0, "pump_hours": 0}


def _find_best_pump_segment(klines: list[dict]) -> dict | None:
    """
    在K线序列中找到最大的上涨段。
    返回该段的涨幅、总成交额、每涨1%的成本。
    """
    n = len(klines)
    best = None

    # 滑动窗口找连续上涨段（允许中间有小回调）
    i = 0
    while i < n:
        # 找上涨起点
        j = i + 1
        low_price = klines[i]["low"]
        high_price = klines[i]["high"]
        total_volume = klines[i]["quote_volume"]
        pullback_count = 0

        while j < n:
            k = klines[j]
            if k["close"] >= k["open"]:  # 阳线
                high_price = max(high_price, k["high"])
                total_volume += k["quote_volume"]
                pullback_count = 0
            else:  # 阴线（回调）
                pullback_count += 1
                total_volume += k["quote_volume"]
                if pullback_count >= 3:  # 连续3根阴线，段结束
                    break
                # 如果回调超过涨幅的50%，段结束
                current_gain = (high_price - low_price) / low_price if low_price > 0 else 0
                pullback = (high_price - k["low"]) / high_price if high_price > 0 else 0
                if current_gain > 0.05 and pullback > current_gain * 0.5:
                    break
            j += 1

        pump_pct = (high_price - low_price) / low_price if low_price > 0 else 0
        hours = j - i

        if pump_pct > 0.10 and hours >= 2:  # 至少涨10%且持续2小时
            cost_per_pct = total_volume / (pump_pct * 100) if pump_pct > 0 else 0

            if best is None or pump_pct > best["max_pump_pct"]:
                best = {
                    "max_pump_pct": pump_pct,
                    "pump_volume": total_volume,
                    "pump_hours": hours,
                    "cost_per_pct": cost_per_pct,
                    "low_price": low_price,
                    "high_price": high_price,
                }

        i = max(j, i + 1)

    return best


async def _estimate_oi_accumulation(symbol: str, current_price: float) -> dict:
    """
    从 48h OI 历史估算 MM 建仓量。
    OI 净增量 × 当前价格 ≈ 新增持仓价值（MM + 散户）
    """
    try:
        oi_hist = await get_oi_history(symbol, ACCUMULATION_WINDOW_MS)
        if len(oi_hist) < 2:
            return {"oi_increase": 0, "estimated_cost": 0, "data_points": 0}

        oi_start = oi_hist[0]["oi"]
        oi_end = oi_hist[-1]["oi"]
        oi_increase = oi_end - oi_start
        oi_change_pct = oi_increase / oi_start if oi_start > 0 else 0

        # OI 增量 × 价格 = 新增持仓名义价值
        # 实际保证金成本 ≈ 名义价值 / 杠杆（假设平均10x）
        nominal_value = abs(oi_increase) * current_price
        estimated_margin = nominal_value / 10

        return {
            "oi_start": oi_start,
            "oi_end": oi_end,
            "oi_increase": oi_increase,
            "oi_change_pct": oi_change_pct,
            "nominal_value": nominal_value,
            "estimated_cost": estimated_margin,
            "data_points": len(oi_hist),
        }
    except Exception:
        return {"oi_increase": 0, "estimated_cost": 0, "data_points": 0}


def _calc_thin_score(current_price: float, asks: list[tuple[float, float]]) -> float:
    """
    卖盘薄度评分 (0-100)。
    越高 = 卖盘越薄 = 越容易拉。
    计算方式：当前价格到 +20% 范围内的卖盘总价值，越少分越高。
    """
    target = current_price * 1.2
    total_ask_value = sum(p * q for p, q in asks if p <= target)

    # 基准：$500K 以下算非常薄，$5M 以上算很厚
    if total_ask_value <= 0:
        return 100.0
    if total_ask_value >= 5_000_000:
        return 10.0
    if total_ask_value <= 500_000:
        return 90.0

    # 线性插值
    score = 90 - (total_ask_value - 500_000) / (5_000_000 - 500_000) * 80
    return max(10.0, min(90.0, score))


def _assess_pump_potential(
    symbol: str,
    current_price: float,
    pump_costs: dict,
    oi_cost: dict,
    thin_score: float,
    ticker: dict,
    concentration: dict,
    kline_cost: dict,
    short_liq: dict,
) -> dict:
    """综合评估拉盘潜力"""
    quote_volume = ticker.get("quote_volume", 0)
    price_change_24h = ticker.get("price_change_pct", 0)

    # MM 已投入成本
    mm_invested = oi_cost.get("estimated_cost", 0)
    oi_change_pct = oi_cost.get("oi_change_pct", 0)

    # 拉到 +100% 的成本
    cost_2x = pump_costs.get("+100%", {}).get("cost", 0)

    # === 拉盘空间估算 ===
    estimated_multiplier = 0.0
    if mm_invested > 0:
        for label, data in sorted(pump_costs.items(), key=lambda x: x[1]["cost"]):
            if data["cost"] > 0 and data["cost"] < mm_invested * 2:
                pct = float(label.replace("+", "").replace("%", "")) / 100
                estimated_multiplier = max(estimated_multiplier, pct)

    # === 清算收益 vs 拉盘成本 ===
    liq_profit_ratio = 0.0
    cost_per_pct = kline_cost.get("cost_per_pct", 0)
    liq_map = short_liq.get("liquidation_map", {})
    if cost_per_pct > 0 and liq_map:
        # 拉到+20%的成本 vs 清算收益
        pump_cost_20 = cost_per_pct * 20
        liq_at_20 = liq_map.get("+20%", {}).get("cumulative_liquidation", 0)
        if pump_cost_20 > 0:
            liq_profit_ratio = liq_at_20 / pump_cost_20

    # === 综合评分 (0-100) ===
    score = 0.0

    # 盘口集中度/MM控盘 (权重 25%)
    score += concentration["score"] * 0.25

    # 空头清算收益 (权重 25%) — 核心动力
    if liq_profit_ratio > 2.0:
        score += 25  # 清算收益远超拉盘成本，MM必拉
    elif liq_profit_ratio > 1.0:
        score += 20  # 清算收益 > 成本，有动力
    elif liq_profit_ratio > 0.5:
        score += 12
    elif short_liq.get("short_ratio", 0) > 0.6:
        score += 15  # 空头占比极高，即使没有历史成本数据也是强信号

    # OI 建仓信号 (权重 20%)
    if oi_change_pct > 0.1:
        score += 20
    elif oi_change_pct > 0.05:
        score += 12
    elif oi_change_pct > 0.02:
        score += 6

    # 拉盘性价比 (权重 15%)
    if cost_2x > 0 and mm_invested > 0:
        ratio = mm_invested / cost_2x
        if ratio > 3:
            score += 15
        elif ratio > 1:
            score += 10
        elif ratio > 0.5:
            score += 5

    # 24h 涨幅信号 (权重 15%)
    if 5 < price_change_24h < 30:
        score += 15
    elif 0 < price_change_24h <= 5:
        score += 8
    elif price_change_24h >= 30:
        score += 3

    score = min(100.0, score)

    # === 进场建议 ===
    if score >= 70:
        advice = "🟢 强烈关注 — MM控盘+空头肥+清算收益高"
        risk = "低"
    elif score >= 50:
        advice = "🟡 值得关注 — 有拉盘条件，等启动信号"
        risk = "中"
    elif score >= 35:
        advice = "🟠 观望 — 条件一般，需要更多确认"
        risk = "中高"
    else:
        advice = "🔴 不建议 — 拉盘条件不足或成本过高"
        risk = "高"

    return {
        "symbol": symbol,
        "type": "拉盘评估",
        "current_price": current_price,
        "pump_costs": pump_costs,
        "oi_accumulation": oi_cost,
        "thin_score": thin_score,
        "concentration": concentration,
        "kline_cost": kline_cost,
        "short_liq": short_liq,
        "liq_profit_ratio": liq_profit_ratio,
        "estimated_multiplier": estimated_multiplier,
        "score": score,
        "advice": advice,
        "risk": risk,
        "quote_volume_24h": quote_volume,
        "price_change_24h": price_change_24h,
    }


import time

# --- 通知状态缓存 ---
# {symbol: {"score": float, "last_push_ts": float, "level": str}}
_notify_cache: dict[str, dict] = {}

# 去重间隔
DEDUP_INTERVAL_GREEN = 30 * 60    # 🟢 30分钟内不重复
DEDUP_INTERVAL_YELLOW = 60 * 60   # 🟡 1小时内不重复
HOURLY_REPORT_INTERVAL = 60 * 60  # 每小时汇总一次
_last_hourly_report = 0.0


async def scan_pump_candidates(symbols: list[str]) -> list[dict]:
    """
    扫描监控币种，分级推送 + 启动信号检测 + 去重。

    推送规则：
    🟢 评分>=70: 立即推送（30min去重）
    🟡 评分>=50: 首次发现或评分跳级时推送（1h去重）
    🟠 评分35-50: 汇总到每小时报告
    启动信号: 评分从<50跳到>=50，或24h涨幅从<5%跳到>10%
    """
    global _last_hourly_report
    now = time.time()
    alerts_to_push = []
    hourly_watchlist = []

    for symbol in symbols:
        result = await estimate_pump_cost(symbol)
        if not result:
            await asyncio.sleep(0.5)
            continue

        score = result["score"]
        prev = _notify_cache.get(symbol, {})
        prev_score = prev.get("score", 0)
        prev_level = prev.get("level", "")
        last_push = prev.get("last_push_ts", 0)

        # 判断当前级别
        if score >= 70:
            level = "green"
        elif score >= 50:
            level = "yellow"
        elif score >= 35:
            level = "orange"
        else:
            level = "none"

        # 检测启动信号
        is_launch = False
        if prev_score < 50 and score >= 50:
            is_launch = True  # 评分跳级
            result["_launch_signal"] = "评分突破50 (从%.0f→%.0f)" % (prev_score, score)
        price_chg = result.get("price_change_24h", 0)
        prev_price_chg = prev.get("price_change_24h", 0)
        if prev_price_chg < 5 and price_chg >= 10:
            is_launch = True
            result["_launch_signal"] = result.get("_launch_signal", "") + " 涨幅启动(%.1f%%→%.1f%%)" % (prev_price_chg, price_chg)

        # 决定是否推送
        should_push = False
        if level == "green":
            if now - last_push >= DEDUP_INTERVAL_GREEN or is_launch:
                should_push = True
        elif level == "yellow":
            if prev_level != "yellow" and prev_level != "green":
                should_push = True  # 首次进入黄色
            elif is_launch:
                should_push = True  # 启动信号
            elif now - last_push >= DEDUP_INTERVAL_YELLOW:
                should_push = True  # 定时更新
        elif level == "orange":
            hourly_watchlist.append(result)

        if is_launch and level in ("green", "yellow"):
            result["type"] = "拉盘评估"
            result["_is_launch"] = True
            should_push = True

        if should_push:
            alerts_to_push.append(result)
            _notify_cache[symbol] = {
                "score": score,
                "last_push_ts": now,
                "level": level,
                "price_change_24h": price_chg,
            }
        else:
            # 更新缓存但不推送
            _notify_cache[symbol] = {
                "score": score,
                "last_push_ts": last_push,
                "level": level,
                "price_change_24h": price_chg,
            }

        await asyncio.sleep(0.5)  # 限流保护

    # 每小时汇总报告
    if hourly_watchlist and now - _last_hourly_report >= HOURLY_REPORT_INTERVAL:
        _last_hourly_report = now
        report = _build_hourly_report(hourly_watchlist)
        if report:
            alerts_to_push.append(report)

    alerts_to_push.sort(key=lambda x: -x.get("score", 0))
    if alerts_to_push:
        logger.info("拉盘推送: %d 条 (🟢%d 🟡%d 📋%d)",
                    len(alerts_to_push),
                    sum(1 for a in alerts_to_push if a.get("score", 0) >= 70),
                    sum(1 for a in alerts_to_push if 50 <= a.get("score", 0) < 70),
                    sum(1 for a in alerts_to_push if a.get("type") == "拉盘监控报告"))
    return alerts_to_push


def _build_hourly_report(watchlist: list[dict]) -> dict | None:
    """构建每小时的观望列表汇总，每个币给出进场建议和庄家成本"""
    if not watchlist:
        return None
    watchlist.sort(key=lambda x: -x["score"])
    items = []
    for r in watchlist[:10]:
        items.append(_build_coin_brief(r))
    return {
        "type": "拉盘监控报告",
        "symbol": "汇总",
        "count": len(watchlist),
        "items": items,
        "score": 0,
    }


def _build_coin_brief(r: dict) -> dict:
    """为单个币种构建进场建议、点位、庄家成本"""
    sym = r["symbol"]
    price = r.get("current_price", 0)
    score = r.get("score", 0)
    sl = r.get("short_liq", {})
    kc = r.get("kline_cost", {})
    conc = r.get("concentration", {})
    lpr = r.get("liq_profit_ratio", 0)
    oi_acc = r.get("oi_accumulation", {})
    price_chg = r.get("price_change_24h", 0)

    short_val = sl.get("short_value", 0)
    short_pct = sl.get("short_ratio", 0)
    cost_per_pct = kc.get("cost_per_pct", 0)
    mm_cost = oi_acc.get("estimated_cost", 0)
    oi_chg = oi_acc.get("oi_change_pct", 0)

    # --- 庄家成本估算 ---
    if mm_cost > 0:
        mm_cost_str = f"${mm_cost:,.0f} (48h OI{oi_chg:+.0%})"
    elif kc.get("pump_volume", 0) > 0:
        est = kc["pump_volume"] * 0.15
        mm_cost_str = f"~${est:,.0f} (按历史量15%估)"
    else:
        mm_cost_str = "数据不足"

    # --- 预估能拉多少 ---
    if cost_per_pct > 0 and short_val > 0:
        liq_map = sl.get("liquidation_map", {})
        best_target_pct = 0
        for label, data in sorted(liq_map.items(), key=lambda x: x[1]["target_pct"]):
            target_pct = data["target_pct"]
            liq_val = data["cumulative_liquidation"]
            pump_cost = cost_per_pct * (target_pct * 100)
            if pump_cost > 0 and liq_val / pump_cost >= 0.3:
                best_target_pct = target_pct
        if best_target_pct > 0:
            target_price = price * (1 + best_target_pct)
            pump_est = f"+{best_target_pct:.0%} → ${target_price:.4f}"
        else:
            pump_est = "空间有限"
    elif short_pct > 0.6:
        pump_est = f"空头肥({short_pct:.0%})，等启动信号"
    else:
        pump_est = "暂无明确信号"

    # --- 进场建议 ---
    if score >= 70:
        if price_chg < 15:
            entry = price * 0.98
            stop = price * 0.90
            advice = f"🟢 可进 | 入场≤${entry:.4f} | 止损${stop:.4f}"
        else:
            entry = price * 0.95
            stop = price * 0.85
            advice = f"🟢 回调进 | 入场≤${entry:.4f} | 止损${stop:.4f}"
    elif score >= 50:
        if price_chg < 10:
            entry = price * 0.97
            stop = price * 0.88
            advice = f"🟡 轻仓试 | 入场≤${entry:.4f} | 止损${stop:.4f}"
        else:
            advice = "🟡 等回调，追高风险大"
    elif short_pct > 0.6 and price_chg < 5:
        advice = "🟠 空头多但未启动，设价格提醒"
    else:
        advice = "🔴 观望"

    return {
        "symbol": sym,
        "score": score,
        "price": price,
        "advice": advice,
        "pump_est": pump_est,
        "mm_cost": mm_cost_str,
        "short_val": short_val,
        "short_pct": short_pct,
        "lpr": lpr,
        "cost_per_pct": cost_per_pct,
        "price_chg": price_chg,
        "mm_controlled": conc.get("is_mm_controlled", False),
    }

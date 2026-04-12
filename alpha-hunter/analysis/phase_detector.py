"""
庄家阶段判断引擎
综合 OI、价格、成交量、多空比等信号，判断当前所处阶段
参考框架：吸筹 → 拉升 → 出货 → 挤压/跑路
"""
import logging
from data.redis_store import get_oi_history, get_price_history, get_long_short_history
import config

logger = logging.getLogger(__name__)

# 分析窗口
ANALYSIS_WINDOW_MS = 4 * 60 * 60 * 1000  # 4小时


async def detect_phase(symbols: list[str]) -> list[dict]:
    """
    综合多维度信号判断每个 symbol 当前所处的庄家操盘阶段。
    返回阶段判断结果列表（仅返回有明确信号的）。
    """
    alerts = []

    for symbol in symbols:
        try:
            oi_hist = await get_oi_history(symbol, ANALYSIS_WINDOW_MS)
            price_hist = await get_price_history(symbol, ANALYSIS_WINDOW_MS)
            lsr_hist = await get_long_short_history(symbol, ANALYSIS_WINDOW_MS)

            if len(oi_hist) < 5 or len(price_hist) < 5:
                continue

            oi_values = [h["oi"] for h in oi_hist]
            price_values = [h["price"] for h in price_hist]

            oi_change = (oi_values[-1] - oi_values[0]) / oi_values[0] if oi_values[0] > 0 else 0
            price_change = (price_values[-1] - price_values[0]) / price_values[0] if price_values[0] > 0 else 0

            # 价格波动率（标准差/均值）
            avg_price = sum(price_values) / len(price_values)
            price_volatility = (sum((p - avg_price) ** 2 for p in price_values) / len(price_values)) ** 0.5 / avg_price if avg_price > 0 else 0

            # 多空比趋势
            lsr_trend = 0.0
            if len(lsr_hist) >= 2:
                lsr_values = [h["ratio"] for h in lsr_hist]
                lsr_trend = (lsr_values[-1] - lsr_values[0]) / lsr_values[0] if lsr_values[0] > 0 else 0

            phase = _classify_phase(oi_change, price_change, price_volatility, lsr_trend)
            if phase and phase["confidence"] >= config.PHASE_CONFIDENCE_THRESHOLD:
                alerts.append({
                    "symbol": symbol,
                    "type": "阶段判断",
                    **phase,
                    "oi_change_4h": oi_change,
                    "price_change_4h": price_change,
                    "volatility_4h": price_volatility,
                })
                logger.info(
                    "阶段判断 %s: %s (置信度 %.0f%%)",
                    symbol, phase["phase"], phase["confidence"] * 100,
                )
        except Exception as e:
            logger.warning("阶段判断 %s 失败: %s", symbol, e)

    return alerts


def _classify_phase(oi_change: float, price_change: float,
                    volatility: float, lsr_trend: float) -> dict | None:
    """
    根据多维度信号分类当前阶段。
    返回 {phase, emoji, desc, confidence, suggestion}
    """
    scores = {
        "吸筹期": 0.0,
        "拉升期": 0.0,
        "出货期": 0.0,
        "挤压期": 0.0,
    }

    # === 吸筹期信号 ===
    # OI 缓慢上升 + 价格低位横盘 + 成交量低迷
    if oi_change > 0.03 and abs(price_change) < 0.02:
        scores["吸筹期"] += 0.4
    if volatility < 0.02:
        scores["吸筹期"] += 0.2
    if oi_change > 0.05 and abs(price_change) < 0.03:
        scores["吸筹期"] += 0.2

    # === 拉升期信号 ===
    # OI 上升 + 价格上涨 + 散户偏空（多空比下降）
    if oi_change > 0.05 and price_change > 0.05:
        scores["拉升期"] += 0.4
    if lsr_trend < -0.1:  # 散户在做空
        scores["拉升期"] += 0.2
    if price_change > 0.1:
        scores["拉升期"] += 0.2

    # === 出货期信号 ===
    # 价格高位横盘/微跌 + OI 下降
    if oi_change < -0.03 and abs(price_change) < 0.03:
        scores["出货期"] += 0.4
    if lsr_trend > 0.1:  # 散户在追多
        scores["出货期"] += 0.2
    if oi_change < -0.05:
        scores["出货期"] += 0.2

    # === 挤压期信号 ===
    # OI 急降 + 价格剧烈波动
    if abs(oi_change) > 0.15 and abs(price_change) > 0.1:
        scores["挤压期"] += 0.5
    if volatility > 0.05:
        scores["挤压期"] += 0.3

    # 找最高分阶段
    best_phase = max(scores, key=scores.get)
    confidence = scores[best_phase]

    if confidence < 0.3:
        return None

    phase_info = {
        "吸筹期": {"emoji": "🔍", "desc": "OI缓慢上升+价格横盘，庄家可能在悄悄建仓",
                   "suggestion": "关注后续放量突破信号"},
        "拉升期": {"emoji": "🚀", "desc": "OI和价格同步上升，趋势行情进行中",
                   "suggestion": "注意散户做空比例，挤压风险"},
        "出货期": {"emoji": "⚠️", "desc": "OI下降+价格横盘，庄家可能在高位出货",
                   "suggestion": "谨慎追多，注意止损"},
        "挤压期": {"emoji": "💥", "desc": "OI急剧变化+价格剧烈波动，挤压正在发生",
                   "suggestion": "极端行情，控制仓位"},
    }

    info = phase_info[best_phase]
    return {
        "phase": best_phase,
        "emoji": info["emoji"],
        "desc": info["desc"],
        "suggestion": info["suggestion"],
        "confidence": min(confidence, 1.0),
    }

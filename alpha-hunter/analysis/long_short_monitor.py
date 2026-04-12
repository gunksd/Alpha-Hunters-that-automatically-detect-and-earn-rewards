"""
多空比监控
检测极端多空比、急剧反转、多空比与价格背离
"""
import asyncio
import logging
from data import binance_futures as bf
from data.redis_store import store_long_short_ratio, get_long_short_history
import config

logger = logging.getLogger(__name__)

_prev_ratio: dict[str, float] = {}


async def check_long_short_anomaly(symbols: list[str]) -> list[dict]:
    """检测多空比异动"""
    global _prev_ratio
    alerts = []

    for symbol in symbols:
        try:
            rows = await bf.fetch_top_long_short_ratio(symbol, period="5m", limit=1)
            if not rows:
                continue

            ratio = rows[0]["long_short_ratio"]
            await store_long_short_ratio(symbol, ratio)

            reasons = []

            # 极端多空比
            if ratio >= config.LS_RATIO_EXTREME_HIGH:
                reasons.append(f"多空比极端偏多 {ratio:.2f}")
            elif ratio <= config.LS_RATIO_EXTREME_LOW:
                reasons.append(f"多空比极端偏空 {ratio:.2f}")

            # 急剧反转
            if symbol in _prev_ratio:
                prev = _prev_ratio[symbol]
                if prev > 0:
                    change = (ratio - prev) / prev
                    if abs(change) >= config.LS_RATIO_REVERSAL_THRESHOLD:
                        direction = "多转空" if change < 0 else "空转多"
                        reasons.append(f"多空比急剧反转 {direction} ({change:+.1%})")

            if reasons:
                alerts.append({
                    "symbol": symbol,
                    "type": "多空比异动",
                    "long_short_ratio": ratio,
                    "prev_ratio": _prev_ratio.get(symbol),
                    "reasons": reasons,
                })
                logger.info("多空比异动 %s: %s", symbol, "; ".join(reasons))

            _prev_ratio[symbol] = ratio
        except Exception as e:
            logger.warning("获取 %s 多空比失败: %s", symbol, e)
        await asyncio.sleep(0.1)

    return alerts

"""
OI（Open Interest）异动检测
增强版：多时间框架 + OI 累积趋势 + Redis 存储
"""
import asyncio
import logging
from data import binance_futures as bf
from data.redis_store import store_oi, get_oi_history
import config

logger = logging.getLogger(__name__)

# 缓存上一轮 OI 值: {symbol: oi_value}
_prev_oi: dict[str, float] = {}


async def check_oi_anomaly(symbols: list[str]) -> list[dict]:
    """
    检测 OI 异动。
    对每个 symbol 获取当前 OI，与上一轮对比，超过阈值则报告。
    同时写入 Redis 存储历史数据。
    """
    global _prev_oi
    alerts = []

    for symbol in symbols:
        try:
            data = await bf.fetch_open_interest(symbol)
            current_oi = data["oi"]

            # 写入 Redis
            await store_oi(symbol, current_oi)

            if symbol in _prev_oi and _prev_oi[symbol] > 0:
                prev = _prev_oi[symbol]
                change_rate = (current_oi - prev) / prev

                if abs(change_rate) >= config.OI_CHANGE_THRESHOLD:
                    direction = "增仓" if change_rate > 0 else "减仓"

                    # 检查 1h OI 趋势
                    oi_1h = await get_oi_history(symbol, 60 * 60 * 1000)
                    trend_1h = ""
                    if len(oi_1h) >= 3:
                        oi_vals = [h["oi"] for h in oi_1h]
                        trend_change = (oi_vals[-1] - oi_vals[0]) / oi_vals[0] if oi_vals[0] > 0 else 0
                        if trend_change > 0.05:
                            trend_1h = "1h持续增仓"
                        elif trend_change < -0.05:
                            trend_1h = "1h持续减仓"

                    alerts.append({
                        "symbol": symbol,
                        "type": "OI异动",
                        "direction": direction,
                        "prev_oi": prev,
                        "current_oi": current_oi,
                        "change_rate": change_rate,
                        "trend_1h": trend_1h,
                    })
                    logger.info(
                        "OI异动 %s: %.2f → %.2f (%+.1f%%) %s %s",
                        symbol, prev, current_oi, change_rate * 100, direction, trend_1h,
                    )

            _prev_oi[symbol] = current_oi
        except Exception as e:
            logger.warning("获取 %s OI 失败: %s", symbol, e)
        await asyncio.sleep(0.1)

    return alerts

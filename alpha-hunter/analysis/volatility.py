"""
价格波动率检测
"""
import asyncio
import logging
from data import binance_futures as bf
import config

logger = logging.getLogger(__name__)


async def check_price_volatility(symbols: list[str]) -> list[dict]:
    """检测 5 分钟价格涨跌幅是否超过阈值"""
    alerts = []

    for symbol in symbols:
        try:
            klines = await bf.fetch_klines(symbol, interval="5m", limit=2)
            if len(klines) < 2:
                continue

            prev_close = klines[-2]["close"]
            curr_close = klines[-1]["close"]

            if prev_close <= 0:
                continue

            change = (curr_close - prev_close) / prev_close

            if abs(change) >= config.PRICE_CHANGE_THRESHOLD:
                alerts.append({
                    "symbol": symbol,
                    "type": "价格波动",
                    "prev_price": prev_close,
                    "current_price": curr_close,
                    "change_rate": change,
                })
                logger.info(
                    "价格波动 %s: %.6f → %.6f (%+.2f%%)",
                    symbol, prev_close, curr_close, change * 100,
                )
        except Exception as e:
            logger.warning("获取 %s 价格数据失败: %s", symbol, e)
        await asyncio.sleep(0.1)

    return alerts

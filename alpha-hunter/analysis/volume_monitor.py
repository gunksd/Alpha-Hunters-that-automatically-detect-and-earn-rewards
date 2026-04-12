"""
成交量突增检测
"""
import asyncio
import logging
from data import binance_futures as bf
import config

logger = logging.getLogger(__name__)


async def check_volume_spike(symbols: list[str]) -> list[dict]:
    """
    检测 5 分钟成交量是否超过过去 1 小时均值的 N 倍。
    使用 5m K 线数据，取最新一根 vs 前 12 根均值。
    """
    alerts = []

    for symbol in symbols:
        try:
            klines = await bf.fetch_klines(symbol, interval="5m", limit=13)
            if len(klines) < 3:
                continue

            latest = klines[-1]
            history = klines[:-1]
            avg_vol = sum(k["quote_volume"] for k in history) / len(history)

            if avg_vol <= 0:
                continue

            ratio = latest["quote_volume"] / avg_vol

            if ratio >= config.VOLUME_SPIKE_MULTIPLIER:
                alerts.append({
                    "symbol": symbol,
                    "type": "成交量突增",
                    "current_volume": latest["quote_volume"],
                    "avg_volume": avg_vol,
                    "ratio": ratio,
                })
                logger.info(
                    "成交量突增 %s: $%.0f (%.1fx 均值 $%.0f)",
                    symbol, latest["quote_volume"], ratio, avg_vol,
                )
        except Exception as e:
            logger.warning("获取 %s K线失败: %s", symbol, e)
        await asyncio.sleep(0.1)  # 避免 klines 端点限流

    return alerts

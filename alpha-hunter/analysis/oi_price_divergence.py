"""
OI + 价格联动分析
检测 OI 与价格之间的背离/联动模式，识别庄家行为阶段
"""
import logging
from data import binance_futures as bf
from data.redis_store import store_oi, store_price, get_oi_history, get_price_history
import config

logger = logging.getLogger(__name__)

# 时间框架定义 (毫秒)
TIMEFRAMES = {
    "5m": 5 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "48h": 48 * 60 * 60 * 1000,
}


def _calc_change_rate(values: list[float]) -> float:
    """计算序列的总变化率"""
    if len(values) < 2 or values[0] == 0:
        return 0.0
    return (values[-1] - values[0]) / values[0]


def _classify_pattern(oi_change: float, price_change: float) -> dict | None:
    """根据 OI 和价格变化率分类模式"""
    oi_thresh = config.OI_PRICE_OI_THRESHOLD
    price_thresh = config.OI_PRICE_PRICE_THRESHOLD

    # OI↑ + Price 横盘/微跌 → 吸筹
    if oi_change > oi_thresh and abs(price_change) < price_thresh:
        return {"pattern": "吸筹信号", "desc": "OI上升但价格横盘，庄家可能在建仓",
                "emoji": "🔍", "severity": "medium"}

    # OI 急降 + Price 急涨 → 空头挤压
    if oi_change < -oi_thresh * 2 and price_change > price_thresh * 2:
        return {"pattern": "空头挤压", "desc": "OI急降+价格急涨，空头被挤压",
                "emoji": "🔥", "severity": "high"}

    # OI 急降 + Price 急跌 → 多头清算
    if oi_change < -oi_thresh * 2 and price_change < -price_thresh * 2:
        return {"pattern": "多头清算", "desc": "OI急降+价格急跌，多头被清算",
                "emoji": "💀", "severity": "high"}

    # OI↑ + Price↑ + 散户偏空 → 拉升期
    if oi_change > oi_thresh and price_change > price_thresh:
        return {"pattern": "趋势拉升", "desc": "OI和价格同步上升，趋势行情",
                "emoji": "🚀", "severity": "medium"}

    # OI↓ + Price 横盘 → 出货信号
    if oi_change < -oi_thresh and abs(price_change) < price_thresh:
        return {"pattern": "出货信号", "desc": "OI下降但价格横盘，庄家可能在出货",
                "emoji": "⚠️", "severity": "high"}

    return None


async def check_oi_price_divergence(symbols: list[str]) -> list[dict]:
    """
    多时间框架 OI-价格联动分析。
    对每个 symbol，在 5m/1h/4h 三个时间框架上检测 OI 与价格的背离模式。
    """
    alerts = []

    for symbol in symbols:
        try:
            # 获取当前 OI 和价格
            oi_data = await bf.fetch_open_interest(symbol)
            current_oi = oi_data["oi"]

            klines = await bf.fetch_klines(symbol, interval="5m", limit=2)
            if not klines:
                continue
            current_price = klines[-1]["close"]

            # 存入 Redis
            await store_oi(symbol, current_oi)
            await store_price(symbol, current_price)

            # 多时间框架分析
            for tf_name, tf_ms in TIMEFRAMES.items():
                oi_hist = await get_oi_history(symbol, tf_ms)
                price_hist = await get_price_history(symbol, tf_ms)

                if len(oi_hist) < 2 or len(price_hist) < 2:
                    continue

                oi_values = [h["oi"] for h in oi_hist]
                price_values = [h["price"] for h in price_hist]

                oi_change = _calc_change_rate(oi_values)
                price_change = _calc_change_rate(price_values)

                pattern = _classify_pattern(oi_change, price_change)
                if pattern:
                    alerts.append({
                        "symbol": symbol,
                        "type": f"OI-价格联动({tf_name})",
                        "pattern": pattern["pattern"],
                        "desc": pattern["desc"],
                        "emoji": pattern["emoji"],
                        "severity": pattern["severity"],
                        "oi_change": oi_change,
                        "price_change": price_change,
                        "timeframe": tf_name,
                    })
                    logger.info(
                        "OI-价格联动 %s [%s]: %s (OI %+.1f%%, Price %+.1f%%)",
                        symbol, tf_name, pattern["pattern"],
                        oi_change * 100, price_change * 100,
                    )
        except Exception as e:
            logger.warning("OI-价格分析 %s 失败: %s", symbol, e)

    return alerts

"""
资金费率异动检测
"""
import logging
from data import binance_futures as bf
import config

logger = logging.getLogger(__name__)

_prev_funding: dict[str, float] = {}


async def check_funding_anomaly(symbols: list[str]) -> list[dict]:
    """检测资金费率异动：绝对值过高 或 变化过大"""
    global _prev_funding
    alerts = []

    try:
        rows = await bf.fetch_premium_index(symbols)
    except Exception as e:
        logger.warning("获取资金费率失败: %s", e)
        return alerts

    for r in rows:
        symbol = r["symbol"]
        rate = r["funding_rate"]
        triggered = False
        reasons = []

        if abs(rate) >= config.FUNDING_RATE_THRESHOLD:
            triggered = True
            reasons.append(f"费率绝对值 {rate*100:.4f}% 超阈值")

        if symbol in _prev_funding:
            delta = rate - _prev_funding[symbol]
            if abs(delta) >= config.FUNDING_RATE_CHANGE:
                triggered = True
                reasons.append(f"费率变化 {delta*100:+.4f}%")

        if triggered:
            alerts.append({
                "symbol": symbol,
                "type": "资金费率异动",
                "funding_rate": rate,
                "prev_rate": _prev_funding.get(symbol),
                "reasons": reasons,
            })
            logger.info("资金费率异动 %s: %s", symbol, "; ".join(reasons))

        _prev_funding[symbol] = rate

    return alerts

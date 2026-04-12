"""
挤压检测器
检测空头挤压（Short Squeeze）和多头挤压（Long Squeeze）
参考 SIREN 操盘案例：OI 持续累积后突然大幅下降 + 价格剧烈波动
"""
import logging
from data.redis_store import get_oi_history, get_price_history
import config

logger = logging.getLogger(__name__)

# 累积期检测窗口
ACCUMULATION_WINDOW_MS = config.SQUEEZE_ACCUMULATION_DAYS * 86400 * 1000
# 挤压检测窗口
SQUEEZE_WINDOW_MS = 60 * 60 * 1000  # 1小时


async def check_squeeze(symbols: list[str]) -> list[dict]:
    """
    检测挤压事件。
    逻辑：
    1. 检查过去 N 天 OI 是否持续累积（趋势上升）
    2. 检查最近 1 小时 OI 是否急剧下降
    3. 同时价格是否剧烈波动
    4. 结合资金费率方向判断挤压类型
    """
    alerts = []

    for symbol in symbols:
        try:
            # 获取长期 OI 历史（累积期）
            long_oi = await get_oi_history(symbol, ACCUMULATION_WINDOW_MS)
            if len(long_oi) < 10:
                continue

            # 获取短期 OI 历史（挤压窗口）
            short_oi = await get_oi_history(symbol, SQUEEZE_WINDOW_MS)
            if len(short_oi) < 2:
                continue

            # 获取短期价格历史
            short_price = await get_price_history(symbol, SQUEEZE_WINDOW_MS)
            if len(short_price) < 2:
                continue

            # 计算长期 OI 趋势（是否持续累积）
            long_oi_values = [h["oi"] for h in long_oi]
            long_oi_start = sum(long_oi_values[:3]) / 3
            long_oi_peak = max(long_oi_values)

            if long_oi_start <= 0:
                continue

            accumulation_rate = (long_oi_peak - long_oi_start) / long_oi_start

            # 计算短期 OI 变化（是否急降）
            short_oi_values = [h["oi"] for h in short_oi]
            short_oi_change = (short_oi_values[-1] - short_oi_values[0]) / short_oi_values[0] if short_oi_values[0] > 0 else 0

            # 计算短期价格变化
            price_values = [h["price"] for h in short_price]
            price_change = (price_values[-1] - price_values[0]) / price_values[0] if price_values[0] > 0 else 0

            # 挤压条件：OI 有累积 + OI 急降 + 价格剧烈波动
            if (accumulation_rate >= config.SQUEEZE_ACCUMULATION_THRESHOLD
                    and short_oi_change <= -config.SQUEEZE_OI_DROP_THRESHOLD
                    and abs(price_change) >= config.SQUEEZE_PRICE_THRESHOLD):

                if price_change > 0:
                    squeeze_type = "空头挤压"
                    emoji = "🔥"
                    desc = f"OI累积{accumulation_rate:.0%}后1h急降{short_oi_change:.0%}，价格暴涨{price_change:.0%}，空头被挤压"
                else:
                    squeeze_type = "多头挤压"
                    emoji = "💀"
                    desc = f"OI累积{accumulation_rate:.0%}后1h急降{short_oi_change:.0%}，价格暴跌{price_change:.0%}，多头被清算"

                alerts.append({
                    "symbol": symbol,
                    "type": squeeze_type,
                    "emoji": emoji,
                    "desc": desc,
                    "accumulation_rate": accumulation_rate,
                    "oi_drop": short_oi_change,
                    "price_change": price_change,
                    "severity": "critical",
                })
                logger.info("🚨 %s %s: %s", squeeze_type, symbol, desc)

        except Exception as e:
            logger.warning("挤压检测 %s 失败: %s", symbol, e)

    return alerts

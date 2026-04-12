"""
涨幅榜异动检测
优先级最高的信号：监控24h涨幅/跌幅榜异动标的
主动MM买壳不会只拉几十个点，敢于追高
"""
import logging
from data import binance_futures as bf
from data.redis_store import get_redis
import config
import json
import time

logger = logging.getLogger(__name__)

# 上一轮涨幅榜快照
_prev_top_gainers: set[str] = set()


async def _store_gainer_event(symbol: str, data: dict) -> None:
    """记录涨幅榜事件到 Redis，用于后续分析"""
    r = await get_redis()
    ts = int(time.time() * 1000)
    event = json.dumps({**data, "ts": ts})
    await r.zadd(f"gainer:{symbol}", {event: ts})
    cutoff = ts - 7 * 86400 * 1000
    await r.zremrangebyscore(f"gainer:{symbol}", "-inf", cutoff)


async def check_gainer_anomaly(watched_symbols: list[str]) -> list[dict]:
    """
    检测涨幅榜异动：
    1. 从全市场 ticker 中找出涨幅/跌幅 top N
    2. 与监控列表交叉，找出在 Alpha∩合约 中的异动标的
    3. 新进入涨幅榜的标的 = 异动信号（刚开始拉盘）
    4. 涨幅超过阈值的标的 = 强势信号
    """
    global _prev_top_gainers
    alerts = []

    try:
        tickers = await bf.fetch_tickers()
    except Exception as e:
        logger.warning("获取行情失败: %s", e)
        return alerts

    watched_set = set(watched_symbols)

    # 按涨幅排序
    sorted_by_change = sorted(tickers, key=lambda x: x["price_change_pct"], reverse=True)

    # 涨幅榜 top N（在监控列表中的）
    top_gainers = []
    for t in sorted_by_change:
        if t["symbol"] in watched_set and t["price_change_pct"] >= config.GAINER_MIN_CHANGE_PCT:
            top_gainers.append(t)
            if len(top_gainers) >= config.GAINER_TOP_N:
                break

    current_gainer_set = {t["symbol"] for t in top_gainers}

    # 新进入涨幅榜的标的（最重要的信号）
    new_gainers = current_gainer_set - _prev_top_gainers
    for t in top_gainers:
        if t["symbol"] in new_gainers:
            alert = {
                "symbol": t["symbol"],
                "type": "涨幅榜异动",
                "sub_type": "新进榜",
                "price_change_pct": t["price_change_pct"],
                "price": t["price"],
                "quote_volume": t["quote_volume"],
                "severity": "high",
            }
            alerts.append(alert)
            await _store_gainer_event(t["symbol"], alert)
            logger.info(
                "🔺 涨幅榜新进 %s: %+.1f%% 价格 %.6f 成交额 $%.0f",
                t["symbol"], t["price_change_pct"], t["price"], t["quote_volume"],
            )

    # 超强涨幅标的（可能是MM启动）
    for t in top_gainers:
        if t["price_change_pct"] >= config.GAINER_STRONG_CHANGE_PCT:
            alert = {
                "symbol": t["symbol"],
                "type": "涨幅榜异动",
                "sub_type": "超强涨幅",
                "price_change_pct": t["price_change_pct"],
                "price": t["price"],
                "quote_volume": t["quote_volume"],
                "severity": "critical",
            }
            # 避免和新进榜重复
            if t["symbol"] not in new_gainers:
                alerts.append(alert)
                await _store_gainer_event(t["symbol"], alert)
            logger.info(
                "🚀 超强涨幅 %s: %+.1f%% 成交额 $%.0f",
                t["symbol"], t["price_change_pct"], t["quote_volume"],
            )

    # 跌幅榜异动（可能是庄家震仓/寻找流动性，参考SIREN案例）
    sorted_by_drop = sorted(tickers, key=lambda x: x["price_change_pct"])
    for t in sorted_by_drop[:config.GAINER_TOP_N]:
        if t["symbol"] in watched_set and t["price_change_pct"] <= -config.GAINER_DROP_ALERT_PCT:
            alerts.append({
                "symbol": t["symbol"],
                "type": "涨幅榜异动",
                "sub_type": "跌幅榜异动",
                "price_change_pct": t["price_change_pct"],
                "price": t["price"],
                "quote_volume": t["quote_volume"],
                "severity": "medium",
            })
            logger.info(
                "🔻 跌幅榜 %s: %.1f%% (可能震仓/寻找流动性)",
                t["symbol"], t["price_change_pct"],
            )

    _prev_top_gainers = current_gainer_set
    return alerts

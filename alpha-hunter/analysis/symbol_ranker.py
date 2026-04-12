"""
标的池优先级排序
优先选择：过去半年新上的币 or 过去有过大幅波动的币
MM买壳更倾向于选这类标的
"""
import logging
import time
from data import binance_futures as bf

logger = logging.getLogger(__name__)

# 缓存上币时间和波动数据
_symbol_info_cache: dict[str, dict] = {}
_cache_ts: float = 0
CACHE_TTL = 3600  # 1小时刷新


async def _load_symbol_info() -> dict[str, dict]:
    """加载交易对信息：上线时间、历史波动"""
    global _symbol_info_cache, _cache_ts

    now = time.time()
    if _symbol_info_cache and (now - _cache_ts) < CACHE_TTL:
        return _symbol_info_cache

    try:
        from httpx import AsyncClient
        async with AsyncClient(base_url="https://fapi.binance.com", timeout=15) as client:
            resp = await client.get("/fapi/v1/exchangeInfo")
            resp.raise_for_status()
            info = resp.json()

        six_months_ago = int((now - 180 * 86400) * 1000)

        for s in info.get("symbols", []):
            if s.get("contractType") != "PERPETUAL" or s.get("quoteAsset") != "USDT":
                continue
            symbol = s["symbol"]
            onboard_date = s.get("onboardDate", 0)
            _symbol_info_cache[symbol] = {
                "onboard_date": onboard_date,
                "is_new": onboard_date >= six_months_ago,
            }

        _cache_ts = now
        logger.info("标的信息加载完成: %d 个, 新币 %d 个",
                     len(_symbol_info_cache),
                     sum(1 for v in _symbol_info_cache.values() if v["is_new"]))
    except Exception as e:
        logger.warning("加载标的信息失败: %s", e)

    return _symbol_info_cache


async def rank_symbols(symbols: list[str]) -> list[str]:
    """
    对监控列表排序，优先级高的排前面：
    1. 过去半年新上的币（MM买壳偏好）
    2. 有历史大波动的币
    3. 其他
    """
    info = await _load_symbol_info()

    # 获取24h行情用于波动评估
    try:
        tickers = await bf.fetch_tickers(symbols)
        ticker_map = {t["symbol"]: t for t in tickers}
    except Exception:
        ticker_map = {}

    def _score(sym: str) -> float:
        score = 0.0
        si = info.get(sym, {})

        # 新币加分
        if si.get("is_new"):
            score += 100

        # 历史大波动加分
        t = ticker_map.get(sym)
        if t:
            vol_pct = abs(t.get("price_change_pct", 0))
            if vol_pct >= 20:
                score += 50
            elif vol_pct >= 10:
                score += 30
            elif vol_pct >= 5:
                score += 10

            # 成交额加分（鱼多的标的）
            qv = t.get("quote_volume", 0)
            if qv >= 50_000_000:
                score += 20
            elif qv >= 10_000_000:
                score += 10

        return score

    ranked = sorted(symbols, key=_score, reverse=True)

    new_count = sum(1 for s in ranked if info.get(s, {}).get("is_new"))
    logger.info("标的排序完成: %d 个, 新币优先 %d 个", len(ranked), new_count)

    return ranked

"""
币安合约数据采集
提供：交易对列表、OI、资金费率、行情、K线、强平订单
"""
import logging
from typing import Any
import httpx
import config

logger = logging.getLogger(__name__)

_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            base_url=config.BINANCE_FUTURES_URL,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
    return _client


async def fetch_futures_symbols() -> list[str]:
    """获取所有 USDT 永续合约交易对 symbol"""
    client = _get_client()
    resp = await client.get("/fapi/v1/exchangeInfo")
    resp.raise_for_status()
    info = resp.json()
    symbols = [
        s["symbol"]
        for s in info.get("symbols", [])
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    ]
    logger.info("获取到 %d 个 USDT 永续合约", len(symbols))
    return symbols


async def fetch_open_interest(symbol: str) -> dict[str, Any]:
    """获取单个交易对的当前 OI"""
    client = _get_client()
    resp = await client.get("/fapi/v1/openInterest", params={"symbol": symbol})
    resp.raise_for_status()
    data = resp.json()
    return {"symbol": symbol, "oi": float(data["openInterest"]), "time": data["time"]}


async def fetch_open_interest_hist(symbol: str, period: str = "5m", limit: int = 2) -> list[dict]:
    """获取 OI 历史数据，用于计算变化率"""
    client = _get_client()
    resp = await client.get(
        "/futures/data/openInterestHist",
        params={"symbol": symbol, "period": period, "limit": limit},
    )
    resp.raise_for_status()
    return [
        {"oi": float(r["sumOpenInterest"]), "oi_value": float(r["sumOpenInterestValue"]), "ts": r["timestamp"]}
        for r in resp.json()
    ]


async def fetch_premium_index(symbols: list[str] | None = None) -> list[dict]:
    """获取资金费率（premiumIndex 包含 lastFundingRate）"""
    client = _get_client()
    resp = await client.get("/fapi/v1/premiumIndex")
    resp.raise_for_status()
    rows = resp.json()
    if symbols:
        sym_set = set(symbols)
        rows = [r for r in rows if r["symbol"] in sym_set]
    return [
        {
            "symbol": r["symbol"],
            "funding_rate": float(r["lastFundingRate"]),
            "mark_price": float(r["markPrice"]),
            "next_funding_time": r["nextFundingTime"],
        }
        for r in rows
    ]


async def fetch_tickers(symbols: list[str] | None = None) -> list[dict]:
    """获取 24h 行情"""
    client = _get_client()
    resp = await client.get("/fapi/v1/ticker/24hr")
    resp.raise_for_status()
    rows = resp.json()
    if symbols:
        sym_set = set(symbols)
        rows = [r for r in rows if r["symbol"] in sym_set]
    return [
        {
            "symbol": r["symbol"],
            "price": float(r["lastPrice"]),
            "price_change_pct": float(r["priceChangePercent"]),
            "volume": float(r["volume"]),
            "quote_volume": float(r["quoteVolume"]),
        }
        for r in rows
    ]


async def fetch_klines(symbol: str, interval: str = "5m", limit: int = 13) -> list[dict]:
    """获取 K 线数据（默认 13 根 5min K 线 = 当前 + 过去 1 小时）"""
    client = _get_client()
    resp = await client.get(
        "/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
    )
    resp.raise_for_status()
    return [
        {
            "open_time": k[0],
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "quote_volume": float(k[7]),
        }
        for k in resp.json()
    ]


async def fetch_force_orders(symbol: str | None = None, limit: int = 20) -> list[dict]:
    """获取最近的强平订单"""
    client = _get_client()
    params: dict[str, Any] = {"limit": limit}
    if symbol:
        params["symbol"] = symbol
    resp = await client.get("/fapi/v1/allForceOrders", params=params)
    resp.raise_for_status()
    return [
        {
            "symbol": r["symbol"],
            "side": r["side"],
            "price": float(r["price"]),
            "qty": float(r["origQty"]),
            "value": float(r["price"]) * float(r["origQty"]),
            "time": r["time"],
        }
        for r in resp.json()
    ]


async def fetch_depth(symbol: str, limit: int = 500) -> dict:
    """获取 Order Book 深度（最多1000档）"""
    client = _get_client()
    resp = await client.get(
        "/fapi/v1/depth",
        params={"symbol": symbol, "limit": limit},
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        "symbol": symbol,
        "bids": [(float(p), float(q)) for p, q in data.get("bids", [])],  # [(price, qty), ...]
        "asks": [(float(p), float(q)) for p, q in data.get("asks", [])],
        "time": data.get("T", 0),
    }


async def fetch_top_long_short_ratio(symbol: str, period: str = "5m", limit: int = 1) -> list[dict]:
    """获取大户多空比（账户数）"""
    client = _get_client()
    resp = await client.get(
        "/futures/data/topLongShortAccountRatio",
        params={"symbol": symbol, "period": period, "limit": limit},
    )
    resp.raise_for_status()
    return [
        {
            "symbol": symbol,
            "long_account": float(r["longAccount"]),
            "short_account": float(r["shortAccount"]),
            "long_short_ratio": float(r["longShortRatio"]),
            "ts": r["timestamp"],
        }
        for r in resp.json()
    ]


async def fetch_global_long_short_ratio(symbol: str, period: str = "5m", limit: int = 1) -> list[dict]:
    """获取全市场多空比（持仓量）"""
    client = _get_client()
    resp = await client.get(
        "/futures/data/globalLongShortAccountRatio",
        params={"symbol": symbol, "period": period, "limit": limit},
    )
    resp.raise_for_status()
    return [
        {
            "symbol": symbol,
            "long_account": float(r["longAccount"]),
            "short_account": float(r["shortAccount"]),
            "long_short_ratio": float(r["longShortRatio"]),
            "ts": r["timestamp"],
        }
        for r in resp.json()
    ]


async def fetch_top_long_short_position_ratio(symbol: str, period: str = "5m", limit: int = 1) -> list[dict]:
    """获取大户多空比（持仓量）"""
    client = _get_client()
    resp = await client.get(
        "/futures/data/topLongShortPositionRatio",
        params={"symbol": symbol, "period": period, "limit": limit},
    )
    resp.raise_for_status()
    return [
        {
            "symbol": symbol,
            "long_account": float(r["longAccount"]),
            "short_account": float(r["shortAccount"]),
            "long_short_ratio": float(r["longShortRatio"]),
            "ts": r["timestamp"],
        }
        for r in resp.json()
    ]

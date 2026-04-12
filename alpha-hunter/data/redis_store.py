"""
Redis 时序数据存储
存储 OI、价格、多空比等历史数据，支持多时间框架分析
"""
import json
import logging
import time
from typing import Any

import redis.asyncio as aioredis

import config

logger = logging.getLogger(__name__)

_pool: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _pool
    if _pool is None:
        _pool = aioredis.from_url(
            config.REDIS_URL,
            decode_responses=True,
            max_connections=config.REDIS_MAX_CONNECTIONS,
        )
    return _pool


async def store_oi(symbol: str, oi: float, oi_value: float = 0.0, ts: int | None = None) -> None:
    """存储 OI 快照到 sorted set"""
    r = await get_redis()
    ts = ts or int(time.time() * 1000)
    data = json.dumps({"oi": oi, "oi_value": oi_value, "ts": ts})
    await r.zadd(f"oi:{symbol}", {data: ts})
    # 清理超过 7 天的数据
    cutoff = ts - 7 * 86400 * 1000
    await r.zremrangebyscore(f"oi:{symbol}", "-inf", cutoff)


async def get_oi_history(symbol: str, lookback_ms: int) -> list[dict]:
    """获取指定时间范围内的 OI 历史"""
    r = await get_redis()
    now = int(time.time() * 1000)
    start = now - lookback_ms
    raw = await r.zrangebyscore(f"oi:{symbol}", start, now)
    return [json.loads(item) for item in raw]


async def store_price(symbol: str, price: float, ts: int | None = None) -> None:
    """存储价格快照"""
    r = await get_redis()
    ts = ts or int(time.time() * 1000)
    data = json.dumps({"price": price, "ts": ts})
    await r.zadd(f"price:{symbol}", {data: ts})
    cutoff = ts - 7 * 86400 * 1000
    await r.zremrangebyscore(f"price:{symbol}", "-inf", cutoff)


async def get_price_history(symbol: str, lookback_ms: int) -> list[dict]:
    """获取指定时间范围内的价格历史"""
    r = await get_redis()
    now = int(time.time() * 1000)
    start = now - lookback_ms
    raw = await r.zrangebyscore(f"price:{symbol}", start, now)
    return [json.loads(item) for item in raw]


async def store_long_short_ratio(symbol: str, ratio: float, ts: int | None = None) -> None:
    """存储多空比快照"""
    r = await get_redis()
    ts = ts or int(time.time() * 1000)
    data = json.dumps({"ratio": ratio, "ts": ts})
    await r.zadd(f"lsr:{symbol}", {data: ts})
    cutoff = ts - 30 * 86400 * 1000
    await r.zremrangebyscore(f"lsr:{symbol}", "-inf", cutoff)


async def get_long_short_history(symbol: str, lookback_ms: int) -> list[dict]:
    """获取指定时间范围内的多空比历史"""
    r = await get_redis()
    now = int(time.time() * 1000)
    start = now - lookback_ms
    raw = await r.zrangebyscore(f"lsr:{symbol}", start, now)
    return [json.loads(item) for item in raw]


async def close() -> None:
    """关闭 Redis 连接"""
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None

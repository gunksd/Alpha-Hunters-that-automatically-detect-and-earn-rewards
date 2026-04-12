"""
币安 Alpha 代币列表数据采集
"""
import logging
import httpx
import config

logger = logging.getLogger(__name__)


async def fetch_alpha_tokens() -> list[str]:
    """获取币安 Alpha 所有代币的 symbol 列表（大写）"""
    url = f"{config.BINANCE_BASE_URL}{config.ALPHA_TOKEN_LIST_PATH}"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        body = resp.json()

    if not body.get("success"):
        logger.error("Alpha API 返回失败: %s", body.get("message", "unknown"))
        return []

    tokens = body.get("data", [])
    symbols = [t["symbol"].upper() for t in tokens if t.get("symbol")]
    logger.info("获取到 %d 个 Alpha 代币", len(symbols))
    return symbols

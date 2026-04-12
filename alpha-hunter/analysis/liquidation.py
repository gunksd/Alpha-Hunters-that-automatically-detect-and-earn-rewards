"""
大单/清算数据监控
注意：allForceOrders REST 端点需要 API Key，清算监控主要依赖 WebSocket 流。
此模块作为备用，仅在配置了 API Key 时生效。
"""
import logging
import config

logger = logging.getLogger(__name__)


async def check_large_liquidations(symbols: list[str]) -> list[dict]:
    """
    清算监控主要通过 WebSocket (data/websocket.py) 实现。
    REST API 的 allForceOrders 需要 API Key 认证，暂不使用。
    """
    # WebSocket 流在 main.py 中通过 listen_liquidations 处理
    return []

"""
币安合约清算流 WebSocket
实时监听全市场强平事件
"""
import asyncio
import json
import logging
from typing import Callable, Awaitable

import websockets

import config

logger = logging.getLogger(__name__)


async def listen_liquidations(
    on_liquidation: Callable[[dict], Awaitable[None]],
    symbols: set[str] | None = None,
):
    """
    连接币安强平 WebSocket 流，收到事件后回调 on_liquidation。
    symbols: 只关注的交易对集合（None 表示全部）
    """
    url = f"{config.BINANCE_WS_URL}/!forceOrder@arr"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("清算 WebSocket 已连接")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        order = msg.get("o", {})
                        sym = order.get("s", "")
                        if symbols and sym not in symbols:
                            continue
                        price = float(order.get("p", 0))
                        qty = float(order.get("q", 0))
                        value = price * qty
                        if value < config.LIQUIDATION_THRESHOLD:
                            continue
                        event = {
                            "symbol": sym,
                            "side": order.get("S", ""),
                            "price": price,
                            "qty": qty,
                            "value": value,
                            "time": msg.get("E", 0),
                        }
                        await on_liquidation(event)
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logger.warning("解析清算消息失败: %s", e)
        except Exception as e:
            logger.error("清算 WebSocket 断开: %s，5秒后重连", e)
            await asyncio.sleep(5)

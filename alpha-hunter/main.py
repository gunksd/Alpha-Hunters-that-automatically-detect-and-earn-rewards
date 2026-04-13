"""
Alpha Hunter — 主调度入口
监控币安Alpha ∩ 合约币种的多维度异动
增强版：OI-价格联动、多空比、挤压检测、庄家阶段判断
"""
import asyncio
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 加载 .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    import os
    for line in _env_path.read_text().strip().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

import config
from data.binance_alpha import fetch_alpha_tokens, fetch_alpha_tokens_with_mcap
from data.binance_futures import fetch_futures_symbols
from data.websocket import listen_liquidations
from data.redis_store import close as close_redis
from analysis.crossfilter import cross_filter
from analysis.oi_monitor import check_oi_anomaly
from analysis.funding_monitor import check_funding_anomaly
from analysis.volume_monitor import check_volume_spike
from analysis.volatility import check_price_volatility
from analysis.liquidation import check_large_liquidations
from analysis.oi_price_divergence import check_oi_price_divergence
from analysis.long_short_monitor import check_long_short_anomaly
from analysis.squeeze_detector import check_squeeze
from analysis.phase_detector import detect_phase
from analysis.gainer_monitor import check_gainer_anomaly
from analysis.pre_squeeze import check_pre_squeeze
from analysis.symbol_ranker import rank_symbols
from analysis.pump_cost import scan_pump_candidates
from alert.wecom import send_alert

CST = timezone(timedelta(hours=8))

logging.basicConfig(level=getattr(logging, config.LOG_LEVEL), format=config.LOG_FORMAT)
logger = logging.getLogger("alpha-hunter")

# 全局状态
watched_symbols: list[str] = []
watched_set: set[str] = set()
last_alpha_refresh = 0.0


async def refresh_watchlist():
    """刷新监控列表：Alpha ∩ 合约 + 市值过滤"""
    global watched_symbols, watched_set, last_alpha_refresh
    logger.info("刷新监控列表...")
    alpha_mcap = await fetch_alpha_tokens_with_mcap()
    futures_symbols = await fetch_futures_symbols()
    watched_symbols = cross_filter(alpha_mcap, futures_symbols)
    # 标的池排序：新币和大波动币优先
    watched_symbols = await rank_symbols(watched_symbols)
    watched_set = set(watched_symbols)
    last_alpha_refresh = asyncio.get_event_loop().time()
    logger.info("当前监控 %d 个币种: %s", len(watched_symbols), ", ".join(watched_symbols[:20]))
    if len(watched_symbols) > 20:
        logger.info("  ... 及其他 %d 个", len(watched_symbols) - 20)


async def run_monitors():
    """运行所有异动检测，收集告警"""
    # 其他模块只写日志/Redis，不推企业微信
    # 只有拉盘评估模块的结果才推送

    # 第零批：涨幅榜异动（写日志，不推送）
    try:
        await check_gainer_anomaly(watched_symbols)
    except Exception as e:
        logger.error("涨幅榜检测异常: %s", e)

    # 第一批：基础指标检测（写日志+Redis，不推送）
    await asyncio.gather(
        check_oi_anomaly(watched_symbols),
        check_funding_anomaly(watched_symbols),
        check_large_liquidations(watched_symbols),
        check_long_short_anomaly(watched_symbols),
        return_exceptions=True,
    )

    # 第二批：K线相关（写日志，不推送）
    for check_fn in (check_volume_spike, check_price_volatility):
        try:
            await check_fn(watched_symbols)
        except Exception:
            pass

    # 第三批：高级分析（写Redis，不推送）
    await asyncio.gather(
        check_oi_price_divergence(watched_symbols),
        check_squeeze(watched_symbols),
        check_pre_squeeze(watched_symbols),
        return_exceptions=True,
    )

    # 第四批：庄家阶段判断（写Redis，不推送）
    try:
        await detect_phase(watched_symbols)
    except Exception:
        pass

    # === 唯一推送：拉盘评估 ===
    pump_alerts = []
    if config.PUMP_SCAN_ENABLED:
        try:
            scan_symbols = watched_symbols[:config.PUMP_SCAN_TOP_N]
            pump_alerts = await scan_pump_candidates(scan_symbols)
        except Exception as e:
            logger.error("拉盘扫描异常: %s", e)

    return pump_alerts


async def on_ws_liquidation(event: dict):
    """WebSocket 清算事件回调，只写日志不推送"""
    if watched_set and event.get("symbol") not in watched_set:
        return
    logger.info("实时清算 %s: %s $%.0f", event["symbol"], event["side"], event.get("value", 0))


async def poll_loop():
    """主轮询循环"""
    await refresh_watchlist()

    while True:
        now = datetime.now(CST).strftime("%H:%M:%S")
        logger.info("--- 轮询开始 %s ---", now)

        # 定期刷新 Alpha 列表
        elapsed = asyncio.get_event_loop().time() - last_alpha_refresh
        if elapsed >= config.ALPHA_REFRESH_INTERVAL:
            await refresh_watchlist()

        if not watched_symbols:
            logger.warning("监控列表为空，等待下一轮")
            await asyncio.sleep(config.POLL_INTERVAL)
            continue

        alerts = await run_monitors()
        if alerts:
            logger.info("本轮检测到 %d 条异动", len(alerts))
            await send_alert(alerts)
        else:
            logger.info("本轮无异动")

        await asyncio.sleep(config.POLL_INTERVAL)


async def main():
    """启动主循环和 WebSocket 监听"""
    logger.info("=" * 50)
    logger.info("Alpha Hunter 启动 (v2)")
    logger.info("轮询间隔: %ds | OI阈值: %.0f%% | 量比: %.1fx | 涨幅榜: >%.0f%%",
                config.POLL_INTERVAL, config.OI_CHANGE_THRESHOLD * 100,
                config.VOLUME_SPIKE_MULTIPLIER, config.GAINER_MIN_CHANGE_PCT)
    logger.info("模块: 涨幅榜 | OI-价格48h | 多空比 | 挤压前兆 | 挤压检测 | 阶段判断 | 标的排序")
    logger.info("Redis: %s", config.REDIS_URL)
    logger.info("=" * 50)

    # 并行启动轮询和 WebSocket
    await asyncio.gather(
        poll_loop(),
        listen_liquidations(on_ws_liquidation, symbols=None),  # 先监听全部，后续按 watched_set 过滤
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Alpha Hunter 已停止")
        asyncio.run(close_redis())
        sys.exit(0)

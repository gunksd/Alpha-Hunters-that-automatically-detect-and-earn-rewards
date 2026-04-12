"""
挤压前兆检测
检测庄家挤压前的准备动作：清理多头 → 吸引空头 → 启动挤压
参考 @derrrrrrrq 的 RAVE 分析：
- 多头follow太离谱 → 去杠杆清理多头 → 吸引空头 → 挤压
- 仓位限额机制：浮盈满了必须先平仓才能继续拉
"""
import logging
from data.redis_store import get_oi_history, get_price_history, get_long_short_history
import config

logger = logging.getLogger(__name__)

# 检测窗口
SHORT_WINDOW_MS = 4 * 60 * 60 * 1000   # 4小时（短期动作）
MEDIUM_WINDOW_MS = 24 * 60 * 60 * 1000  # 24小时（中期趋势）


async def check_pre_squeeze(symbols: list[str]) -> list[dict]:
    """
    检测挤压前兆模式：

    模式1 - 清理多头阶段：
    - 多空比从极端偏多开始下降（散户多头被清理）
    - 短期价格下跌（去杠杆砸盘）
    - OI 不减反增或持平（空头进场填补）

    模式2 - 空头积累阶段：
    - 价格横盘/微跌后空头大量进场
    - OI 持续上升但价格不涨
    - 空头均价接近当前价格（挤压空间大）

    模式3 - 仓位腾挪信号：
    - 短期内 OI 先降后升（庄家平仓腾空间后重新建仓）
    """
    alerts = []

    for symbol in symbols:
        try:
            # 获取历史数据
            oi_short = await get_oi_history(symbol, SHORT_WINDOW_MS)
            price_short = await get_price_history(symbol, SHORT_WINDOW_MS)
            lsr_short = await get_long_short_history(symbol, SHORT_WINDOW_MS)

            if len(oi_short) < 3 or len(price_short) < 3:
                continue

            oi_vals = [h["oi"] for h in oi_short]
            price_vals = [h["price"] for h in price_short]

            oi_change = (oi_vals[-1] - oi_vals[0]) / oi_vals[0] if oi_vals[0] > 0 else 0
            price_change = (price_vals[-1] - price_vals[0]) / price_vals[0] if price_vals[0] > 0 else 0

            # === 模式1：清理多头阶段 ===
            if len(lsr_short) >= 3:
                lsr_vals = [h["ratio"] for h in lsr_short]
                lsr_start = lsr_vals[0]
                lsr_end = lsr_vals[-1]
                lsr_peak = max(lsr_vals)

                # 多空比从高位回落 + 价格下跌 + OI 不减
                if (lsr_peak >= config.PRE_SQUEEZE_LSR_PEAK
                        and lsr_end < lsr_peak * 0.8
                        and price_change < -config.PRE_SQUEEZE_PRICE_DIP
                        and oi_change > -0.03):

                    alerts.append({
                        "symbol": symbol,
                        "type": "挤压前兆",
                        "pattern": "清理多头",
                        "desc": f"多空比从{lsr_peak:.1f}回落到{lsr_end:.1f}，价格跌{price_change:.1%}但OI变化{oi_change:+.1%}，庄家在清理多头跟风盘",
                        "lsr_peak": lsr_peak,
                        "lsr_current": lsr_end,
                        "price_change": price_change,
                        "oi_change": oi_change,
                        "severity": "high",
                    })
                    logger.info("⚡ 挤压前兆[清理多头] %s: LSR %.1f→%.1f, Price %+.1%%, OI %+.1%%",
                                symbol, lsr_peak, lsr_end, price_change * 100, oi_change * 100)

            # === 模式2：空头积累阶段 ===
            # OI 上升 + 价格横盘/微跌 = 空头在进场
            if (oi_change >= config.PRE_SQUEEZE_OI_ACCUMULATION
                    and price_change <= config.PRE_SQUEEZE_PRICE_FLAT):

                # 检查24h趋势确认
                oi_medium = await get_oi_history(symbol, MEDIUM_WINDOW_MS)
                if len(oi_medium) >= 5:
                    oi_med_vals = [h["oi"] for h in oi_medium]
                    oi_24h_change = (oi_med_vals[-1] - oi_med_vals[0]) / oi_med_vals[0] if oi_med_vals[0] > 0 else 0

                    if oi_24h_change >= config.PRE_SQUEEZE_OI_ACCUMULATION * 2:
                        alerts.append({
                            "symbol": symbol,
                            "type": "挤压前兆",
                            "pattern": "空头积累",
                            "desc": f"OI 24h涨{oi_24h_change:.1%}但价格仅{price_change:+.1%}，空头大量进场，挤压空间积累中",
                            "oi_change_4h": oi_change,
                            "oi_change_24h": oi_24h_change,
                            "price_change": price_change,
                            "severity": "high",
                        })
                        logger.info("⚡ 挤压前兆[空头积累] %s: OI 24h %+.1%%, Price %+.1%%",
                                    symbol, oi_24h_change * 100, price_change * 100)

            # === 模式3：仓位腾挪 ===
            # OI 先降后升（V形）= 庄家平仓腾空间后重新建仓
            if len(oi_vals) >= 5:
                mid = len(oi_vals) // 2
                first_half_change = (oi_vals[mid] - oi_vals[0]) / oi_vals[0] if oi_vals[0] > 0 else 0
                second_half_change = (oi_vals[-1] - oi_vals[mid]) / oi_vals[mid] if oi_vals[mid] > 0 else 0

                if (first_half_change <= -config.PRE_SQUEEZE_POSITION_SHIFT
                        and second_half_change >= config.PRE_SQUEEZE_POSITION_SHIFT):
                    alerts.append({
                        "symbol": symbol,
                        "type": "挤压前兆",
                        "pattern": "仓位腾挪",
                        "desc": f"OI先降{first_half_change:.1%}后升{second_half_change:+.1%}，庄家可能在腾挪仓位空间",
                        "oi_first_half": first_half_change,
                        "oi_second_half": second_half_change,
                        "severity": "medium",
                    })
                    logger.info("⚡ 挤压前兆[仓位腾挪] %s: OI %+.1%% → %+.1%%",
                                symbol, first_half_change * 100, second_half_change * 100)

        except Exception as e:
            logger.warning("挤压前兆检测 %s 失败: %s", symbol, e)

    return alerts

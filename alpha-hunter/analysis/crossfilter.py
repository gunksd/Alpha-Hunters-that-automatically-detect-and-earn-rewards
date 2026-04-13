"""
交叉筛选：币安 Alpha ∩ 币安合约 + 市值过滤
"""
import logging
import config

logger = logging.getLogger(__name__)


def cross_filter(
    alpha_mcap: dict[str, float],
    futures_symbols: list[str],
) -> list[str]:
    """
    返回同时在 Alpha 列表和合约列表中、且市值在范围内的币种。
    alpha_mcap: {SYMBOL: market_cap} 从 fetch_alpha_tokens_with_mcap 获取
    futures_symbols: 合约交易对列表（如 DOGEUSDT）
    """
    cap_min = config.MARKET_CAP_MIN
    cap_max = config.MARKET_CAP_MAX

    matched = []
    filtered_by_cap = 0
    for fs in futures_symbols:
        if not fs.endswith("USDT"):
            continue
        base = fs[:-4]
        if base not in alpha_mcap:
            continue

        mcap = alpha_mcap[base]
        # 市值过滤
        if cap_max > 0 and mcap > cap_max:
            filtered_by_cap += 1
            continue
        if cap_min > 0 and mcap < cap_min:
            filtered_by_cap += 1
            continue

        matched.append(fs)

    logger.info(
        "交叉筛选: %d 个匹配, %d 个被市值过滤掉 (范围: $%s - $%s)",
        len(matched), filtered_by_cap,
        _fmt_money(cap_min), _fmt_money(cap_max),
    )
    return matched


def _fmt_money(v: float) -> str:
    if v <= 0:
        return "不限"
    if v >= 1e9:
        return f"{v/1e9:.0f}B"
    if v >= 1e6:
        return f"{v/1e6:.0f}M"
    return f"{v:,.0f}"

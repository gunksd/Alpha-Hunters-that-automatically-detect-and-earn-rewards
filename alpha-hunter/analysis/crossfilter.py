"""
交叉筛选：币安 Alpha ∩ 币安合约
"""
import logging

logger = logging.getLogger(__name__)


def cross_filter(alpha_symbols: list[str], futures_symbols: list[str]) -> list[str]:
    """
    返回同时在 Alpha 列表和合约列表中的币种。
    Alpha symbol 是纯 token 名（如 DOGE），合约 symbol 带后缀（如 DOGEUSDT）。
    """
    alpha_set = set(alpha_symbols)
    matched = []
    for fs in futures_symbols:
        # 去掉 USDT 后缀得到 base token
        if fs.endswith("USDT"):
            base = fs[:-4]
            if base in alpha_set:
                matched.append(fs)
    logger.info("交叉筛选结果: %d 个币种同时在 Alpha 和合约", len(matched))
    for s in matched:
        logger.debug("  %s", s)
    return matched

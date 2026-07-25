"""Shared deterministic arithmetic for financial ratios."""

from decimal import Context, Decimal, localcontext


RATIO_PRECISION = 28
_RATIO_CONTEXT = Context(prec=RATIO_PRECISION)


def decimal_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    """Divide exact finite Decimals under the scanner's fixed local policy."""
    if type(numerator) is not Decimal or type(denominator) is not Decimal:
        raise TypeError("ratio operands must be exact Decimal values")
    if not numerator.is_finite() or not denominator.is_finite():
        raise ValueError("ratio operands must be finite")
    if denominator == 0:
        raise ValueError("ratio denominator must be nonzero")
    with localcontext(_RATIO_CONTEXT):
        return numerator / denominator

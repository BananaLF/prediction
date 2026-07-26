from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class FeeSchedule:
    rate: Decimal
    exponent: int
    taker_only: bool
    captured_at_ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.rate, Decimal):
            raise TypeError("rate must be Decimal")
        if not self.rate.is_finite():
            raise ValueError("rate must be finite")
        if self.rate < 0:
            raise ValueError("rate must be nonnegative")
        if isinstance(self.exponent, bool) or not isinstance(self.exponent, int):
            raise TypeError("exponent must be an integer")
        if self.exponent <= 0:
            raise ValueError("exponent must be positive")
        if not isinstance(self.taker_only, bool):
            raise TypeError("taker_only must be bool")
        if isinstance(self.captured_at_ms, bool) or not isinstance(self.captured_at_ms, int):
            raise TypeError("captured_at_ms must be an integer")
        if self.captured_at_ms < 0:
            raise ValueError("captured_at_ms must be nonnegative")

    def taker_fee(self, shares: Decimal, price: Decimal) -> Decimal:
        if not isinstance(shares, Decimal) or not isinstance(price, Decimal):
            raise TypeError("shares and price must be Decimal")
        if not shares.is_finite() or not price.is_finite():
            raise ValueError("shares and price must be finite")
        if shares <= 0:
            raise ValueError("shares must be positive")
        if not Decimal("0") < price < Decimal("1"):
            raise ValueError("price must be strictly between 0 and 1")
        return shares * self.rate * (price * (Decimal("1") - price)) ** self.exponent

from dataclasses import dataclass
from decimal import Decimal

from predmarket.domain import BookLevel, Side


class InsufficientDepth(ValueError):
    """Raised when an order book cannot completely fill a requested quantity."""


@dataclass(frozen=True)
class Fill:
    quantity: Decimal
    gross: Decimal
    worst_price: Decimal

    def __post_init__(self) -> None:
        values = (self.quantity, self.gross, self.worst_price)
        if not all(isinstance(value, Decimal) for value in values):
            raise TypeError("quantity, gross, and worst_price must be Decimal")
        if not all(value.is_finite() for value in values):
            raise ValueError("quantity, gross, and worst_price must be finite")


@dataclass(frozen=True)
class OrderBook:
    token_id: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    tick_size: Decimal
    minimum_order_size: Decimal
    exchange_ts_ms: int
    book_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.token_id, str) or not isinstance(self.book_hash, str):
            raise TypeError("token_id and book_hash must be strings")
        if not self.token_id or not self.book_hash:
            raise ValueError("token_id and book_hash must be non-empty")
        if not isinstance(self.tick_size, Decimal) or not isinstance(
            self.minimum_order_size, Decimal
        ):
            raise TypeError("tick_size and minimum_order_size must be Decimal")
        if not self.tick_size.is_finite() or not self.minimum_order_size.is_finite():
            raise ValueError("tick_size and minimum_order_size must be finite")
        if self.tick_size <= 0 or self.minimum_order_size <= 0:
            raise ValueError("tick_size and minimum_order_size must be positive")
        if isinstance(self.exchange_ts_ms, bool) or not isinstance(self.exchange_ts_ms, int):
            raise TypeError("exchange_ts_ms must be an integer")
        if self.exchange_ts_ms < 0:
            raise ValueError("exchange_ts_ms must be nonnegative")
        if not isinstance(self.bids, tuple) or not isinstance(self.asks, tuple):
            raise TypeError("bids and asks must be tuples")
        if not all(isinstance(item, BookLevel) for item in self.bids + self.asks):
            raise TypeError("bids and asks must contain only BookLevel values")

        bid_prices = tuple(item.price for item in self.bids)
        ask_prices = tuple(item.price for item in self.asks)
        if bid_prices != tuple(sorted(bid_prices, reverse=True)):
            raise ValueError("bids must be ordered by descending price")
        if ask_prices != tuple(sorted(ask_prices)):
            raise ValueError("asks must be ordered by ascending price")
        if len(set(bid_prices)) != len(bid_prices) or len(set(ask_prices)) != len(ask_prices):
            raise ValueError("duplicate price levels are not allowed")
        if self.bids and self.asks and self.bids[0].price >= self.asks[0].price:
            raise ValueError("book must not be locked or crossed")

    def walk(self, side: Side, quantity: Decimal) -> Fill:
        if not isinstance(side, Side):
            raise TypeError("side must be a Side")
        if not isinstance(quantity, Decimal):
            raise TypeError("quantity must be Decimal")
        if not quantity.is_finite():
            raise ValueError("quantity must be finite")
        if quantity < self.minimum_order_size:
            raise ValueError("quantity must meet minimum_order_size")

        levels = self.asks if side is Side.BUY else self.bids
        remaining = quantity
        gross = Decimal("0")
        worst_price: Decimal | None = None
        for level in levels:
            consumed = min(remaining, level.size)
            gross += consumed * level.price
            remaining -= consumed
            if consumed:
                worst_price = level.price
            if remaining == 0:
                return Fill(quantity=quantity, gross=gross, worst_price=worst_price)

        raise InsufficientDepth(f"insufficient depth to fill quantity {quantity}")

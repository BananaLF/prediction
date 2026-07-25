from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OpportunityStatus(str, Enum):
    REJECTED = "REJECTED"
    RESEARCH_CANDIDATE = "RESEARCH_CANDIDATE"
    SNAPSHOT_EXECUTABLE = "SNAPSHOT_EXECUTABLE"


class PathKind(str, Enum):
    IMMEDIATE_CONVERSION = "IMMEDIATE_CONVERSION"
    HOLD_TO_RESOLUTION = "HOLD_TO_RESOLUTION"


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    size: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.price, Decimal) or not isinstance(self.size, Decimal):
            raise TypeError("price and size must be Decimal")
        if not Decimal("0") < self.price < Decimal("1"):
            raise ValueError("price must be strictly between 0 and 1")
        if self.size <= Decimal("0"):
            raise ValueError("size must be positive")

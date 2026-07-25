from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from predmarket.domain import PathKind, Side


class ActionKind(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    SPLIT = "SPLIT"
    MERGE = "MERGE"
    NEG_RISK_CONVERT = "NEG_RISK_CONVERT"
    REDEEM = "REDEEM"


@dataclass(frozen=True)
class Action:
    kind: ActionKind
    token_id: str | None = None
    side: Side | None = None
    units: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if not isinstance(self.kind, ActionKind):
            raise TypeError("kind must be an ActionKind")
        if not isinstance(self.units, Decimal):
            raise TypeError("units must be Decimal")
        if not self.units.is_finite() or self.units <= 0:
            raise ValueError("units must be finite and positive")

        expected_side = {
            ActionKind.BUY: Side.BUY,
            ActionKind.SELL: Side.SELL,
        }.get(self.kind)
        if expected_side is not None:
            if not isinstance(self.token_id, str):
                raise ValueError("trading actions require token_id")
            if not self.token_id:
                raise ValueError("token_id must be non-empty")
            if self.side is not expected_side:
                raise ValueError("trading action side must match its kind")
        elif self.token_id is not None or self.side is not None:
            raise ValueError("conversion actions do not accept token_id or side")


@dataclass(frozen=True)
class ActionPath:
    path_id: str
    kind: PathKind
    actions: tuple[Action, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.path_id, str):
            raise TypeError("path_id must be a string")
        if not self.path_id:
            raise ValueError("path_id must be non-empty")
        if not isinstance(self.kind, PathKind):
            raise TypeError("kind must be a PathKind")
        if not isinstance(self.actions, tuple):
            raise TypeError("actions must be a tuple")
        if not self.actions:
            raise ValueError("actions must be non-empty")
        if not all(isinstance(action, Action) for action in self.actions):
            raise TypeError("actions must contain only Action values")


def _binary_tokens(yes_token_id: str, no_token_id: str) -> None:
    if not isinstance(yes_token_id, str) or not isinstance(no_token_id, str):
        raise TypeError("token IDs must be strings")
    if not yes_token_id or not no_token_id:
        raise ValueError("token IDs must be non-empty")
    if yes_token_id == no_token_id:
        raise ValueError("token IDs must differ")


def binary_underpriced_path(yes_token_id: str, no_token_id: str) -> ActionPath:
    _binary_tokens(yes_token_id, no_token_id)
    return ActionPath(
        f"binary-underpriced:{yes_token_id}:{no_token_id}",
        PathKind.IMMEDIATE_CONVERSION,
        (
            Action(ActionKind.BUY, yes_token_id, Side.BUY),
            Action(ActionKind.BUY, no_token_id, Side.BUY),
            Action(ActionKind.MERGE),
        ),
    )


def binary_overpriced_path(yes_token_id: str, no_token_id: str) -> ActionPath:
    _binary_tokens(yes_token_id, no_token_id)
    return ActionPath(
        f"binary-overpriced:{yes_token_id}:{no_token_id}",
        PathKind.IMMEDIATE_CONVERSION,
        (
            Action(ActionKind.SPLIT),
            Action(ActionKind.SELL, yes_token_id, Side.SELL),
            Action(ActionKind.SELL, no_token_id, Side.SELL),
        ),
    )

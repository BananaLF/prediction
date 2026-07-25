# Structural Arbitrage Scanner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a read-only Polymarket scanner that detects, confirms, risk-checks, stores, replays, and reports structural-arbitrage opportunities using executable order-book depth.

**Architecture:** A Python modular monolith separates immutable domain types, order-book simulation, rule/action planning, risk assessment, market-data adapters, SQLite persistence, and CLI orchestration. `scan-once` uses confirmed REST snapshots; `watch` uses WebSocket discovery followed by REST reconfirmation. Every financial value uses `Decimal`, and only `SNAPSHOT_EXECUTABLE` opportunities trigger desktop notifications.

**Tech Stack:** Python 3.10+, `httpx`, `websockets`, `PyYAML`, `aiosqlite`, `pytest`, `pytest-asyncio`, `hypothesis`, SQLite WAL.

---

## File Map

Create or replace these focused units:

```text
pyproject.toml                         Packaging, dependencies, CLI entry point
config/default.yaml                   Approved default thresholds
predmarket/config.py                  Typed configuration loading
predmarket/domain.py                  Immutable shared domain types
predmarket/orderbook.py               Decimal L2 book and depth walking
predmarket/fees.py                    Fee schedule and fee calculation
predmarket/relations.py               Rule schema, validation, and payoff states
predmarket/actions.py                 BUY/SELL/SPLIT/MERGE/CONVERT/REDEEM plans
predmarket/simulator.py               Capital, proceeds, and quantity optimization
predmarket/risk.py                    Partial-fill and hard-risk gates
predmarket/latency.py                 Timestamp and freshness checks
predmarket/epochs.py                  Local order-book epoch lifecycle
predmarket/storage.py                 SQLite schema, transactions, and replay bundles
predmarket/polymarket/gamma.py        Market discovery adapter
predmarket/polymarket/clob.py         REST books and fee adapter
predmarket/polymarket/ws.py           WebSocket receiver and bounded queue
predmarket/engine.py                  Candidate → confirm → simulate → assess pipeline
predmarket/notifier.py                Terminal and macOS notification adapter
predmarket/cli.py                     User-facing commands
predmarket/commands.py                Dependency wiring for CLI commands
rules/example-implication.yaml        Audited rule-file example
tests/fixtures/*.json                 Frozen official-shaped API messages
tests/unit/*.py                       Deterministic component tests
tests/integration/*.py                Adapter, reconnect, and pipeline tests
```

Delete only after replacement tests pass:

```text
predmarket/core.py                    Superseded by domain/orderbook/simulator/risk
predmarket/api.py                     Superseded by polymarket adapters
predmarket/ledger.py                  Superseded by storage/replay
tests/test_core.py                    Superseded by focused tests
```

## Task 1: Package, configuration, and immutable domain types

**Files:**
- Create: `pyproject.toml`
- Create: `config/default.yaml`
- Create: `predmarket/config.py`
- Create: `predmarket/domain.py`
- Create: `tests/unit/test_config.py`
- Create: `tests/unit/test_domain.py`
- Modify: `predmarket/__init__.py`

- [ ] **Step 1: Write failing configuration and Decimal-domain tests**

```python
# tests/unit/test_config.py
from decimal import Decimal
from predmarket.config import Settings


def test_default_risk_limits_are_exact_decimals():
    settings = Settings.load("config/default.yaml")
    assert settings.bankroll == Decimal("1000")
    assert settings.minimum_return == Decimal("0.0075")
    assert settings.safety_buffer_rate == Decimal("0.0025")
    assert settings.max_leg_failure_loss == Decimal("5")
    assert settings.max_unhedged_notional == Decimal("20")
```

```python
# tests/unit/test_domain.py
from decimal import Decimal
import pytest
from predmarket.domain import BookLevel, OpportunityStatus, Side


def test_book_level_rejects_float_and_nonpositive_values():
    with pytest.raises(TypeError):
        BookLevel(price=0.5, size=Decimal("10"))
    with pytest.raises(ValueError):
        BookLevel(price=Decimal("0.5"), size=Decimal("0"))


def test_opportunity_status_names_are_stable():
    assert [x.value for x in OpportunityStatus] == [
        "REJECTED", "RESEARCH_CANDIDATE", "SNAPSHOT_EXECUTABLE"
    ]
    assert Side.BUY.value == "BUY"
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
pytest tests/unit/test_config.py tests/unit/test_domain.py -v
```

Expected: collection fails with `ModuleNotFoundError` for `predmarket.config` or missing domain types.

- [ ] **Step 3: Add packaging and exact default configuration**

```toml
# pyproject.toml
[build-system]
requires = ["setuptools>=69"]
build-backend = "setuptools.build_meta"

[project]
name = "predmarket"
version = "0.2.0"
requires-python = ">=3.10"
dependencies = [
  "aiosqlite>=0.20,<1",
  "httpx>=0.27,<1",
  "PyYAML>=6,<7",
  "websockets>=12,<16",
]

[project.optional-dependencies]
test = [
  "hypothesis>=6,<7",
  "pytest>=8,<9",
  "pytest-asyncio>=0.23,<1",
]

[project.scripts]
predmarket = "predmarket.cli:main"

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
```

```yaml
# config/default.yaml
bankroll: "1000"
minimum_return: "0.0075"
safety_buffer_rate: "0.0025"
max_leg_failure_loss: "5"
max_unhedged_notional: "20"
default_simulation_quantity: "10"
conversion_cost: "0"
maximum_book_age_ms: 1000
maximum_leg_skew_ms: 250
maximum_processing_latency_ms: 100
reconcile_interval_seconds: 30
queue_capacity: 10000
database_path: "data/predmarket.sqlite3"
```

- [ ] **Step 4: Implement typed settings and immutable domain primitives**

```python
# predmarket/config.py
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
import yaml


@dataclass(frozen=True)
class Settings:
    bankroll: Decimal
    minimum_return: Decimal
    safety_buffer_rate: Decimal
    max_leg_failure_loss: Decimal
    max_unhedged_notional: Decimal
    default_simulation_quantity: Decimal
    conversion_cost: Decimal
    maximum_book_age_ms: int
    maximum_leg_skew_ms: int
    maximum_processing_latency_ms: int
    reconcile_interval_seconds: int
    queue_capacity: int
    database_path: Path

    @classmethod
    def load(cls, path: str | Path) -> "Settings":
        raw = yaml.safe_load(Path(path).read_text())
        return cls(
            bankroll=Decimal(raw["bankroll"]),
            minimum_return=Decimal(raw["minimum_return"]),
            safety_buffer_rate=Decimal(raw["safety_buffer_rate"]),
            max_leg_failure_loss=Decimal(raw["max_leg_failure_loss"]),
            max_unhedged_notional=Decimal(raw["max_unhedged_notional"]),
            default_simulation_quantity=Decimal(raw["default_simulation_quantity"]),
            conversion_cost=Decimal(raw["conversion_cost"]),
            maximum_book_age_ms=int(raw["maximum_book_age_ms"]),
            maximum_leg_skew_ms=int(raw["maximum_leg_skew_ms"]),
            maximum_processing_latency_ms=int(raw["maximum_processing_latency_ms"]),
            reconcile_interval_seconds=int(raw["reconcile_interval_seconds"]),
            queue_capacity=int(raw["queue_capacity"]),
            database_path=Path(raw["database_path"]),
        )
```

```python
# predmarket/domain.py
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
            raise ValueError("price must be between zero and one")
        if self.size <= 0:
            raise ValueError("size must be positive")
```

- [ ] **Step 5: Run tests, then commit**

Run:

```bash
pytest tests/unit/test_config.py tests/unit/test_domain.py -v
```

Expected: `4 passed`.

Commit:

```bash
git add pyproject.toml config/default.yaml predmarket/config.py predmarket/domain.py tests/unit/test_config.py tests/unit/test_domain.py predmarket/__init__.py
git commit -m "build: establish scanner domain and configuration"
```

## Task 2: Decimal order-book walking and fee schedules

**Files:**
- Create: `predmarket/orderbook.py`
- Create: `predmarket/fees.py`
- Create: `tests/unit/test_orderbook.py`
- Create: `tests/unit/test_fees.py`

- [ ] **Step 1: Write failing depth-walk and fee tests**

```python
# tests/unit/test_orderbook.py
from decimal import Decimal
import pytest
from predmarket.domain import BookLevel, Side
from predmarket.orderbook import OrderBook, InsufficientDepth


def test_buy_walks_lowest_asks_and_sell_walks_highest_bids():
    book = OrderBook(
        token_id="yes",
        bids=(BookLevel(Decimal(".48"), Decimal("5")), BookLevel(Decimal(".47"), Decimal("10"))),
        asks=(BookLevel(Decimal(".51"), Decimal("4")), BookLevel(Decimal(".52"), Decimal("10"))),
        tick_size=Decimal(".01"),
        minimum_order_size=Decimal("1"),
        exchange_ts_ms=1000,
        book_hash="h1",
    )
    assert book.walk(Side.BUY, Decimal("6")).gross == Decimal("3.08")
    assert book.walk(Side.SELL, Decimal("6")).gross == Decimal("2.87")
    with pytest.raises(InsufficientDepth):
        book.walk(Side.BUY, Decimal("20"))
```

```python
# tests/unit/test_fees.py
from decimal import Decimal
from predmarket.fees import FeeSchedule


def test_fee_schedule_uses_decimal_curve():
    schedule = FeeSchedule(rate=Decimal(".05"), exponent=1, taker_only=True, captured_at_ms=10)
    assert schedule.taker_fee(shares=Decimal("10"), price=Decimal(".5")) == Decimal(".1250")
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/unit/test_orderbook.py tests/unit/test_fees.py -v
```

Expected: import errors for `predmarket.orderbook` and `predmarket.fees`.

- [ ] **Step 3: Implement deterministic order-book walking**

```python
# predmarket/orderbook.py
from dataclasses import dataclass
from decimal import Decimal
from predmarket.domain import BookLevel, Side


class InsufficientDepth(ValueError):
    pass


@dataclass(frozen=True)
class Fill:
    quantity: Decimal
    gross: Decimal
    worst_price: Decimal


@dataclass(frozen=True)
class OrderBook:
    token_id: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    tick_size: Decimal
    minimum_order_size: Decimal
    exchange_ts_ms: int
    book_hash: str

    def walk(self, side: Side, quantity: Decimal) -> Fill:
        if quantity < self.minimum_order_size:
            raise ValueError("quantity below minimum order size")
        levels = sorted(
            self.asks if side is Side.BUY else self.bids,
            key=lambda x: x.price,
            reverse=side is Side.SELL,
        )
        remaining = quantity
        gross = Decimal("0")
        worst = Decimal("0")
        for level in levels:
            take = min(remaining, level.size)
            gross += take * level.price
            worst = level.price
            remaining -= take
            if remaining == 0:
                return Fill(quantity=quantity, gross=gross, worst_price=worst)
        raise InsufficientDepth(f"{self.token_id} lacks depth for {quantity}")
```

- [ ] **Step 4: Implement fee schedule with snapshottable parameters**

```python
# predmarket/fees.py
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class FeeSchedule:
    rate: Decimal
    exponent: int
    taker_only: bool
    captured_at_ms: int

    def taker_fee(self, shares: Decimal, price: Decimal) -> Decimal:
        curve = (price * (Decimal("1") - price)) ** self.exponent
        return shares * self.rate * curve
```

- [ ] **Step 5: Run unit tests and monotonic property tests, then commit**

Add:

```python
# append to tests/unit/test_orderbook.py
from hypothesis import given, strategies as st


@given(st.decimals(min_value="0.01", max_value="0.49", places=2))
def test_raising_ask_never_reduces_buy_cost(price):
    p = Decimal(price)
    low = OrderBook("t", (), (BookLevel(p, Decimal("10")),), Decimal(".01"), Decimal("1"), 1, "a")
    high = OrderBook("t", (), (BookLevel(p + Decimal(".01"), Decimal("10")),), Decimal(".01"), Decimal("1"), 1, "b")
    assert high.walk(Side.BUY, Decimal("2")).gross >= low.walk(Side.BUY, Decimal("2")).gross
```

Run:

```bash
pytest tests/unit/test_orderbook.py tests/unit/test_fees.py -v
```

Expected: `4 passed`.

Commit:

```bash
git add predmarket/orderbook.py predmarket/fees.py tests/unit/test_orderbook.py tests/unit/test_fees.py
git commit -m "feat: model executable depth and fee schedules"
```

## Task 3: Audited relation files and payoff-state validation

**Files:**
- Create: `predmarket/relations.py`
- Create: `rules/example-implication.yaml`
- Create: `tests/unit/test_relations.py`

- [ ] **Step 1: Write failing tests for structural and semantic gates**

```python
# tests/unit/test_relations.py
from pathlib import Path
import pytest
from predmarket.relations import RelationStatus, load_relation, RelationValidationError


def test_a_implies_b_has_three_allowed_states_and_unit_weights():
    relation = load_relation(Path("rules/example-implication.yaml"))
    assert relation.status is RelationStatus.ACTIVE
    assert len(relation.states) == 3
    assert [leg.weight for leg in relation.legs] == [1, 1]
    assert relation.minimum_units_received() == 1


def test_active_relation_requires_human_semantic_certification(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("""
id: bad
version: 1
status: active
legs: [{token_id: no_a, weight: 1}, {token_id: yes_b, weight: 1}]
states: [{name: only, proceeds: {no_a: 1, yes_b: 0}}]
""")
    with pytest.raises(RelationValidationError, match="semantic_review"):
        load_relation(path)
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/unit/test_relations.py -v
```

Expected: import error for `predmarket.relations`.

- [ ] **Step 3: Add a complete reviewed implication example**

```yaml
# rules/example-implication.yaml
id: a-implies-b
version: 1
status: active
kind: implication
source_rules_hash: "example-reviewed-hash"
semantic_review:
  reviewer: "human"
  reviewed_at: "2026-07-26T00:00:00Z"
  conclusion: "A=YES,B=NO is excluded by both settlement rules"
legs:
  - token_id: no_a
    weight: 1
  - token_id: yes_b
    weight: 1
states:
  - name: a_no_b_no
    proceeds: {no_a: 1, yes_b: 0}
  - name: a_no_b_yes
    proceeds: {no_a: 1, yes_b: 1}
  - name: a_yes_b_yes
    proceeds: {no_a: 0, yes_b: 1}
```

- [ ] **Step 4: Implement atomic parsing and unit-weight validation**

```python
# predmarket/relations.py
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import yaml


class RelationValidationError(ValueError):
    pass


class RelationStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"


@dataclass(frozen=True)
class RelationLeg:
    token_id: str
    weight: int


@dataclass(frozen=True)
class RelationState:
    name: str
    proceeds: dict[str, int]


@dataclass(frozen=True)
class Relation:
    relation_id: str
    version: int
    status: RelationStatus
    source_rules_hash: str
    legs: tuple[RelationLeg, ...]
    states: tuple[RelationState, ...]

    def minimum_units_received(self) -> int:
        return min(sum(state.proceeds[leg.token_id] * leg.weight for leg in self.legs) for state in self.states)


def load_relation(path: Path) -> Relation:
    raw = yaml.safe_load(path.read_text())
    status = RelationStatus(raw["status"])
    if status is RelationStatus.ACTIVE and not raw.get("semantic_review"):
        raise RelationValidationError("active relation requires semantic_review")
    legs = tuple(RelationLeg(str(x["token_id"]), int(x["weight"])) for x in raw["legs"])
    if not legs or any(x.weight != 1 for x in legs):
        raise RelationValidationError("first version accepts unit weights only")
    token_ids = {x.token_id for x in legs}
    states = tuple(RelationState(str(x["name"]), dict(x["proceeds"])) for x in raw["states"])
    if not states or any(set(x.proceeds) != token_ids for x in states):
        raise RelationValidationError("every state must cover every leg")
    if any(value not in (0, 1) for state in states for value in state.proceeds.values()):
        raise RelationValidationError("state proceeds must be zero or one")
    return Relation(str(raw["id"]), int(raw["version"]), status, str(raw["source_rules_hash"]), legs, states)
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/unit/test_relations.py -v
```

Expected: `2 passed`.

Commit:

```bash
git add predmarket/relations.py rules/example-implication.yaml tests/unit/test_relations.py
git commit -m "feat: validate audited structural relations"
```

## Task 4: Action paths and immediate binary arbitrage

**Files:**
- Create: `predmarket/actions.py`
- Create: `predmarket/simulator.py`
- Create: `tests/unit/test_simulator.py`

- [ ] **Step 1: Write failing tests for both binary directions**

```python
# tests/unit/test_simulator.py
from decimal import Decimal
from predmarket.actions import ActionKind, binary_overpriced_path, binary_underpriced_path
from predmarket.domain import BookLevel
from predmarket.fees import FeeSchedule
from predmarket.orderbook import OrderBook
from predmarket.simulator import simulate_path


ZERO_FEE = FeeSchedule(Decimal("0"), 1, True, 1)


def book(token, bid, ask, size="100"):
    return OrderBook(
        token, (BookLevel(Decimal(bid), Decimal(size)),),
        (BookLevel(Decimal(ask), Decimal(size)),),
        Decimal(".01"), Decimal("1"), 1000, token,
    )


def test_buy_yes_no_then_merge_is_profitable_after_buffer():
    result = simulate_path(
        binary_underpriced_path("yes", "no"),
        {"yes": book("yes", ".48", ".49"), "no": book("no", ".47", ".48")},
        {"yes": ZERO_FEE, "no": ZERO_FEE},
        quantity=Decimal("10"),
        safety_buffer_rate=Decimal(".0025"),
        conversion_cost=Decimal("0"),
    )
    assert result.actions[-1].kind is ActionKind.MERGE
    assert result.minimum_profit > 0


def test_split_then_sell_yes_no_uses_bids():
    result = simulate_path(
        binary_overpriced_path("yes", "no"),
        {"yes": book("yes", ".52", ".53"), "no": book("no", ".51", ".52")},
        {"yes": ZERO_FEE, "no": ZERO_FEE},
        quantity=Decimal("10"),
        safety_buffer_rate=Decimal(".0025"),
        conversion_cost=Decimal("0"),
    )
    assert result.actions[0].kind is ActionKind.SPLIT
    assert result.minimum_profit > 0
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/unit/test_simulator.py -v
```

Expected: missing `predmarket.actions` or `predmarket.simulator`.

- [ ] **Step 3: Implement explicit action paths**

```python
# predmarket/actions.py
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


@dataclass(frozen=True)
class ActionPath:
    path_id: str
    kind: PathKind
    actions: tuple[Action, ...]


def binary_underpriced_path(yes: str, no: str) -> ActionPath:
    return ActionPath("binary-underpriced", PathKind.IMMEDIATE_CONVERSION, (
        Action(ActionKind.BUY, yes, Side.BUY),
        Action(ActionKind.BUY, no, Side.BUY),
        Action(ActionKind.MERGE),
    ))


def binary_overpriced_path(yes: str, no: str) -> ActionPath:
    return ActionPath("binary-overpriced", PathKind.IMMEDIATE_CONVERSION, (
        Action(ActionKind.SPLIT),
        Action(ActionKind.SELL, yes, Side.SELL),
        Action(ActionKind.SELL, no, Side.SELL),
    ))
```

- [ ] **Step 4: Implement path cash-flow simulation**

```python
# predmarket/simulator.py
from dataclasses import dataclass
from decimal import Decimal
from predmarket.actions import Action, ActionKind, ActionPath
from predmarket.fees import FeeSchedule
from predmarket.orderbook import OrderBook


@dataclass(frozen=True)
class SimulationResult:
    actions: tuple[Action, ...]
    maximum_capital_used: Decimal
    minimum_received: Decimal
    minimum_profit: Decimal
    minimum_return: Decimal


def simulate_path(
    path: ActionPath,
    books: dict[str, OrderBook],
    fees: dict[str, FeeSchedule],
    quantity: Decimal,
    safety_buffer_rate: Decimal,
    conversion_cost: Decimal,
) -> SimulationResult:
    cash = Decimal("0")
    minimum_cash = Decimal("0")
    trading_notional = Decimal("0")
    for action in path.actions:
        if action.kind is ActionKind.SPLIT:
            cash -= quantity + conversion_cost
        elif action.kind is ActionKind.MERGE:
            cash += quantity - conversion_cost
        elif action.kind in (ActionKind.BUY, ActionKind.SELL):
            fill = books[action.token_id].walk(action.side, quantity * action.units)
            average_price = fill.gross / fill.quantity
            fee = fees[action.token_id].taker_fee(fill.quantity, average_price)
            trading_notional += fill.gross
            cash += fill.gross - fee if action.kind is ActionKind.SELL else -(fill.gross + fee)
        minimum_cash = min(minimum_cash, cash)
    cash -= trading_notional * safety_buffer_rate
    capital = -minimum_cash + trading_notional * safety_buffer_rate
    return SimulationResult(path.actions, capital, capital + cash, cash, cash / capital)
```

- [ ] **Step 5: Run tests and commit**

Before running, add breakpoint optimization:

```python
# append to predmarket/simulator.py
from predmarket.orderbook import InsufficientDepth


def optimize_quantities(
    path: ActionPath,
    books: dict[str, OrderBook],
    fees: dict[str, FeeSchedule],
    safety_buffer_rate: Decimal,
    conversion_cost: Decimal,
    bankroll: Decimal,
) -> tuple[SimulationResult, ...]:
    breakpoints = {book.minimum_order_size for book in books.values()}
    for book in books.values():
        running = Decimal("0")
        for level in (*book.asks, *book.bids):
            running += level.size
            breakpoints.add(running)
    results: list[SimulationResult] = []
    for quantity in sorted(breakpoints):
        try:
            result = simulate_path(path, books, fees, quantity, safety_buffer_rate, conversion_cost)
        except (ValueError, InsufficientDepth):
            continue
        if result.maximum_capital_used <= bankroll:
            results.append(result)
    return tuple(results)
```

Add the import and test:

```python
from predmarket.simulator import optimize_quantities


def test_optimizer_only_returns_executable_breakpoints_within_bankroll():
    path = binary_underpriced_path("yes", "no")
    results = optimize_quantities(
        path,
        {"yes": book("yes", ".48", ".49", "4"), "no": book("no", ".47", ".48", "6")},
        {"yes": ZERO_FEE, "no": ZERO_FEE},
        Decimal(".0025"), Decimal("0"), Decimal("1000"),
    )
    assert results
    assert all(x.maximum_capital_used <= Decimal("1000") for x in results)
```

Run:

```bash
pytest tests/unit/test_simulator.py -v
```

Expected: `3 passed`.

Commit:

```bash
git add predmarket/actions.py predmarket/simulator.py tests/unit/test_simulator.py
git commit -m "feat: simulate binary split and merge arbitrage"
```

## Task 5: Hold-to-resolution and Neg-risk action planning

**Files:**
- Modify: `predmarket/actions.py`
- Modify: `predmarket/simulator.py`
- Create: `tests/unit/test_hold_paths.py`

- [ ] **Step 1: Write failing implication and Neg-risk tests**

```python
# tests/unit/test_hold_paths.py
from decimal import Decimal
from predmarket.actions import ActionKind, implication_path, neg_risk_complete_set_path
from predmarket.relations import load_relation
from predmarket.simulator import minimum_relation_received


def test_implication_minimum_received_uses_worst_allowed_state():
    relation = load_relation(__import__("pathlib").Path("rules/example-implication.yaml"))
    assert minimum_relation_received(relation, Decimal("100")) == Decimal("100")
    path = implication_path(relation)
    assert [x.kind for x in path.actions] == [ActionKind.BUY, ActionKind.BUY, ActionKind.REDEEM]


def test_neg_risk_path_requires_explicit_conversion_metadata():
    path = neg_risk_complete_set_path(("yes_a", "yes_b", "yes_c"), conversion_enabled=True)
    assert path.actions[-1].kind is ActionKind.NEG_RISK_CONVERT
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/unit/test_hold_paths.py -v
```

Expected: missing `implication_path`, `neg_risk_complete_set_path`, and `minimum_relation_received`.

- [ ] **Step 3: Add explicit planners without semantic inference**

```python
# append to predmarket/actions.py
from predmarket.relations import Relation


def implication_path(relation: Relation) -> ActionPath:
    actions = tuple(Action(ActionKind.BUY, leg.token_id, Side.BUY) for leg in relation.legs)
    return ActionPath(relation.relation_id, PathKind.HOLD_TO_RESOLUTION, actions + (Action(ActionKind.REDEEM),))


def neg_risk_complete_set_path(tokens: tuple[str, ...], conversion_enabled: bool) -> ActionPath:
    if not conversion_enabled:
        raise ValueError("neg-risk conversion must be explicitly enabled by metadata")
    actions = tuple(Action(ActionKind.BUY, token, Side.BUY) for token in tokens)
    return ActionPath("neg-risk-complete-set", PathKind.IMMEDIATE_CONVERSION, actions + (
        Action(ActionKind.NEG_RISK_CONVERT),
    ))
```

- [ ] **Step 4: Add deterministic worst-state proceeds**

```python
# append to predmarket/simulator.py
from predmarket.relations import Relation


def minimum_relation_received(relation: Relation, quantity: Decimal) -> Decimal:
    units = Decimal(relation.minimum_units_received())
    return units * quantity
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/unit/test_hold_paths.py tests/unit/test_relations.py -v
```

Expected: `4 passed`.

Commit:

```bash
git add predmarket/actions.py predmarket/simulator.py tests/unit/test_hold_paths.py
git commit -m "feat: plan audited logical and neg-risk paths"
```

## Task 6: Partial-fill stress testing and opportunity classification

**Files:**
- Create: `predmarket/risk.py`
- Create: `tests/unit/test_risk.py`

- [ ] **Step 1: Write failing hard-gate tests**

```python
# tests/unit/test_risk.py
from decimal import Decimal
from predmarket.domain import OpportunityStatus
from predmarket.risk import RiskInputs, assess_risk


def base(**overrides):
    values = dict(
        mathematical_return=Decimal(".01"),
        data_valid=True,
        worst_leg_failure_loss=Decimal("4"),
        max_unhedged_notional=Decimal("19"),
        immediate_unwind_known=True,
        unresolved_rule_risk=False,
        unresolved_conversion_risk=False,
        unresolved_settlement_risk=False,
        release_date_known=True,
    )
    values.update(overrides)
    return RiskInputs(**values)


def test_executable_requires_every_hard_gate():
    assert assess_risk(base(), Decimal(".0075"), Decimal("5"), Decimal("20")).status is OpportunityStatus.SNAPSHOT_EXECUTABLE
    assert assess_risk(base(worst_leg_failure_loss=Decimal("5.01")), Decimal(".0075"), Decimal("5"), Decimal("20")).status is OpportunityStatus.REJECTED


def test_unknown_unwind_or_release_date_is_research_only():
    assert assess_risk(base(immediate_unwind_known=False), Decimal(".0075"), Decimal("5"), Decimal("20")).status is OpportunityStatus.RESEARCH_CANDIDATE
    assert assess_risk(base(release_date_known=False), Decimal(".0075"), Decimal("5"), Decimal("20")).status is OpportunityStatus.RESEARCH_CANDIDATE
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/unit/test_risk.py -v
```

Expected: import error for `predmarket.risk`.

- [ ] **Step 3: Implement explicit risk inputs and ordered gates**

```python
# predmarket/risk.py
from dataclasses import dataclass
from decimal import Decimal
from predmarket.domain import OpportunityStatus


@dataclass(frozen=True)
class RiskInputs:
    mathematical_return: Decimal
    data_valid: bool
    worst_leg_failure_loss: Decimal
    max_unhedged_notional: Decimal
    immediate_unwind_known: bool
    unresolved_rule_risk: bool
    unresolved_conversion_risk: bool
    unresolved_settlement_risk: bool
    release_date_known: bool


@dataclass(frozen=True)
class RiskAssessment:
    status: OpportunityStatus
    reasons: tuple[str, ...]


def assess_risk(inputs: RiskInputs, minimum_return: Decimal, max_loss: Decimal, max_unhedged: Decimal) -> RiskAssessment:
    rejected: list[str] = []
    research: list[str] = []
    if inputs.mathematical_return < minimum_return:
        rejected.append("minimum_return")
    if not inputs.data_valid:
        rejected.append("data_invalid")
    if inputs.worst_leg_failure_loss > max_loss:
        rejected.append("leg_failure_loss")
    if inputs.max_unhedged_notional > max_unhedged:
        rejected.append("unhedged_notional")
    if inputs.unresolved_rule_risk:
        rejected.append("rule_risk")
    if inputs.unresolved_conversion_risk:
        rejected.append("conversion_risk")
    if inputs.unresolved_settlement_risk:
        rejected.append("settlement_risk")
    if not inputs.immediate_unwind_known:
        research.append("unwind_unknown")
    if not inputs.release_date_known:
        research.append("release_date_unknown")
    if rejected:
        return RiskAssessment(OpportunityStatus.REJECTED, tuple(rejected + research))
    if research:
        return RiskAssessment(OpportunityStatus.RESEARCH_CANDIDATE, tuple(research))
    return RiskAssessment(OpportunityStatus.SNAPSHOT_EXECUTABLE, ())
```

- [ ] **Step 4: Add permutation-based partial-fill loss tests**

```python
# append to tests/unit/test_risk.py
from predmarket.risk import worst_partial_fill


def test_worst_partial_fill_checks_each_single_leg_unwind():
    result = worst_partial_fill(
        entry_costs={"a": Decimal("38"), "b": Decimal("57")},
        immediate_unwind_values={"a": Decimal("35"), "b": Decimal("55")},
    )
    assert result.worst_leg_failure_loss == Decimal("3")
    assert result.max_unhedged_notional == Decimal("57")
```

Implement:

```python
# append to predmarket/risk.py
@dataclass(frozen=True)
class PartialFillRisk:
    worst_leg_failure_loss: Decimal
    max_unhedged_notional: Decimal


def worst_partial_fill(entry_costs: dict[str, Decimal], immediate_unwind_values: dict[str, Decimal]) -> PartialFillRisk:
    if set(entry_costs) != set(immediate_unwind_values):
        raise ValueError("every leg requires an immediate unwind value")
    losses = [max(Decimal("0"), entry_costs[k] - immediate_unwind_values[k]) for k in entry_costs]
    return PartialFillRisk(max(losses, default=Decimal("0")), max(entry_costs.values(), default=Decimal("0")))
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/unit/test_risk.py -v
```

Expected: `3 passed`.

Commit:

```bash
git add predmarket/risk.py tests/unit/test_risk.py
git commit -m "feat: gate opportunities on partial-fill risk"
```

## Task 7: Latency budgets and order-book epochs

**Files:**
- Create: `predmarket/latency.py`
- Create: `predmarket/epochs.py`
- Create: `tests/unit/test_latency.py`
- Create: `tests/unit/test_epochs.py`

- [ ] **Step 1: Write failing freshness and epoch tests**

```python
# tests/unit/test_latency.py
from predmarket.latency import Timing, validate_timings


def test_timing_rejects_stale_skewed_or_slow_books():
    valid = [Timing(1000, 1050, 1.0, 1.05), Timing(1100, 1120, 1.1, 1.15)]
    assert validate_timings(valid, now_ms=1150, max_age_ms=1000, max_skew_ms=250, max_processing_ms=100).valid
    slow = [Timing(1000, 1050, 1.0, 1.2)]
    assert not validate_timings(slow, 1150, 1000, 250, 100).valid
```

```python
# tests/unit/test_epochs.py
from predmarket.epochs import EpochBook, EpochState


def test_epoch_never_returns_to_live_without_full_snapshot():
    epoch = EpochBook("yes")
    epoch.invalidate("queue_overflow")
    assert epoch.state is EpochState.RESYNC
    epoch.apply_delta("0.50", "10", "BUY", 1000)
    assert epoch.state is EpochState.RESYNC
    epoch.replace_snapshot(snapshot_hash="h2", exchange_ts_ms=1100)
    assert epoch.state is EpochState.LIVE
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/unit/test_latency.py tests/unit/test_epochs.py -v
```

Expected: import errors for latency and epochs.

- [ ] **Step 3: Implement monotonic processing checks**

```python
# predmarket/latency.py
from dataclasses import dataclass


@dataclass(frozen=True)
class Timing:
    exchange_ts_ms: int
    received_ts_ms: int
    received_monotonic: float
    evaluated_monotonic: float


@dataclass(frozen=True)
class TimingAssessment:
    valid: bool
    reasons: tuple[str, ...]


def validate_timings(items: list[Timing], now_ms: int, max_age_ms: int, max_skew_ms: int, max_processing_ms: int) -> TimingAssessment:
    reasons: list[str] = []
    if not items:
        return TimingAssessment(False, ("missing_timing",))
    if any(now_ms - x.exchange_ts_ms > max_age_ms for x in items):
        reasons.append("stale")
    if max(x.exchange_ts_ms for x in items) - min(x.exchange_ts_ms for x in items) > max_skew_ms:
        reasons.append("leg_skew")
    if any((x.evaluated_monotonic - x.received_monotonic) * 1000 > max_processing_ms for x in items):
        reasons.append("processing_latency")
    return TimingAssessment(not reasons, tuple(reasons))
```

- [ ] **Step 4: Implement fail-closed epoch transitions**

```python
# predmarket/epochs.py
from dataclasses import dataclass
from enum import Enum


class EpochState(str, Enum):
    WARMING = "WARMING"
    LIVE = "LIVE"
    STALE = "STALE"
    RESYNC = "RESYNC"


@dataclass
class EpochBook:
    token_id: str
    state: EpochState = EpochState.WARMING
    snapshot_hash: str | None = None
    exchange_ts_ms: int | None = None
    invalid_reason: str | None = None

    def invalidate(self, reason: str) -> None:
        self.state = EpochState.RESYNC
        self.invalid_reason = reason

    def apply_delta(self, price: str, size: str, side: str, exchange_ts_ms: int) -> None:
        if self.state is not EpochState.LIVE:
            return
        if self.exchange_ts_ms is not None and exchange_ts_ms < self.exchange_ts_ms:
            self.invalidate("timestamp_regression")
            return
        self.exchange_ts_ms = exchange_ts_ms

    def replace_snapshot(self, snapshot_hash: str, exchange_ts_ms: int) -> None:
        self.snapshot_hash = snapshot_hash
        self.exchange_ts_ms = exchange_ts_ms
        self.invalid_reason = None
        self.state = EpochState.LIVE
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/unit/test_latency.py tests/unit/test_epochs.py -v
```

Expected: `2 passed`.

Commit:

```bash
git add predmarket/latency.py predmarket/epochs.py tests/unit/test_latency.py tests/unit/test_epochs.py
git commit -m "feat: fail closed on stale order-book epochs"
```

## Task 8: SQLite evidence bundles and deterministic replay

**Files:**
- Create: `predmarket/storage.py`
- Create: `tests/unit/test_storage.py`

- [ ] **Step 1: Write a failing atomic evidence and replay test**

```python
# tests/unit/test_storage.py
import json
import pytest
from predmarket.storage import Store


@pytest.mark.asyncio
async def test_evidence_bundle_round_trips_atomically(tmp_path):
    store = await Store.open(tmp_path / "test.sqlite3")
    bundle = {
        "engine_version": "0.2.0",
        "relation": {"id": "binary-underpriced", "version": 1},
        "books": [{"token_id": "yes", "hash": "h1"}],
        "fee_schedules": [{"token_id": "yes", "rate": "0"}],
        "actions": [{"kind": "BUY", "token_id": "yes"}],
        "risk": {"status": "SNAPSHOT_EXECUTABLE"},
        "economics": {"minimum_return": "0.01"},
    }
    opportunity_id = await store.save_opportunity(bundle)
    assert json.loads(await store.load_replay_bundle(opportunity_id)) == bundle
    await store.close()
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/unit/test_storage.py -v
```

Expected: import error for `predmarket.storage`.

- [ ] **Step 3: Implement WAL schema and versioned evidence JSON**

```python
# predmarket/storage.py
from pathlib import Path
import json
import uuid
import aiosqlite


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS markets (id TEXT PRIMARY KEY, event_id TEXT, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS tokens (id TEXT PRIMARY KEY, market_id TEXT NOT NULL, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fee_schedules (id INTEGER PRIMARY KEY, token_id TEXT NOT NULL, captured_at_ms INTEGER NOT NULL, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS relation_states (relation_id TEXT NOT NULL, relation_version INTEGER NOT NULL, name TEXT NOT NULL, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS relation_payoffs (relation_id TEXT NOT NULL, relation_version INTEGER NOT NULL, state_name TEXT NOT NULL, token_id TEXT NOT NULL, units INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS book_epochs (id TEXT PRIMARY KEY, token_id TEXT NOT NULL, state TEXT NOT NULL, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orderbook_snapshots (id TEXT PRIMARY KEY, epoch_id TEXT NOT NULL, token_id TEXT NOT NULL, exchange_ts_ms INTEGER NOT NULL, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orderbook_levels (snapshot_id TEXT NOT NULL, side TEXT NOT NULL, price TEXT NOT NULL, size TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS opportunities (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    evidence_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS opportunity_legs (opportunity_id TEXT NOT NULL, ordinal INTEGER NOT NULL, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS opportunity_actions (opportunity_id TEXT NOT NULL, ordinal INTEGER NOT NULL, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS risk_assessments (opportunity_id TEXT PRIMARY KEY, status TEXT NOT NULL, body_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    mode TEXT NOT NULL,
    metrics_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS latency_metrics (run_id TEXT NOT NULL, metric TEXT NOT NULL, value_ms REAL NOT NULL);
CREATE TABLE IF NOT EXISTS notifications (opportunity_id TEXT NOT NULL, channel TEXT NOT NULL, success INTEGER NOT NULL, detail TEXT);
CREATE TABLE IF NOT EXISTS relations (
    id TEXT NOT NULL,
    version INTEGER NOT NULL,
    status TEXT NOT NULL,
    body_json TEXT NOT NULL,
    PRIMARY KEY (id, version)
);
"""


class Store:
    def __init__(self, db: aiosqlite.Connection):
        self.db = db

    @classmethod
    async def open(cls, path: str | Path) -> "Store":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        db = await aiosqlite.connect(path)
        await db.executescript(SCHEMA)
        await db.commit()
        return cls(db)

    async def save_opportunity(self, bundle: dict) -> str:
        opportunity_id = uuid.uuid4().hex
        payload = json.dumps(bundle, sort_keys=True, separators=(",", ":"))
        async with self.db.execute("BEGIN IMMEDIATE"):
            await self.db.execute(
                "INSERT INTO opportunities(id,status,engine_version,evidence_json) VALUES(?,?,?,?)",
                (opportunity_id, bundle["risk"]["status"], bundle["engine_version"], payload),
            )
        await self.db.commit()
        return opportunity_id

    async def load_replay_bundle(self, opportunity_id: str) -> str:
        cursor = await self.db.execute("SELECT evidence_json FROM opportunities WHERE id=?", (opportunity_id,))
        row = await cursor.fetchone()
        if row is None:
            raise KeyError(opportunity_id)
        return row[0]

    async def close(self) -> None:
        await self.db.close()
```

- [ ] **Step 4: Add rollback behavior**

```python
# append to tests/unit/test_storage.py
@pytest.mark.asyncio
async def test_invalid_bundle_creates_no_partial_record(tmp_path):
    store = await Store.open(tmp_path / "test.sqlite3")
    with pytest.raises(KeyError):
        await store.save_opportunity({"risk": {"status": "REJECTED"}})
    cursor = await store.db.execute("SELECT COUNT(*) FROM opportunities")
    assert (await cursor.fetchone())[0] == 0
    await store.close()
```

Update `save_opportunity`:

```python
    async def save_opportunity(self, bundle: dict) -> str:
        opportunity_id = uuid.uuid4().hex
        payload = json.dumps(bundle, sort_keys=True, separators=(",", ":"))
        try:
            await self.db.execute("BEGIN IMMEDIATE")
            await self.db.execute(
                "INSERT INTO opportunities(id,status,engine_version,evidence_json) VALUES(?,?,?,?)",
                (opportunity_id, bundle["risk"]["status"], bundle["engine_version"], payload),
            )
        except Exception:
            await self.db.rollback()
            raise
        await self.db.commit()
        return opportunity_id
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/unit/test_storage.py -v
```

Expected: `2 passed`.

Commit:

```bash
git add predmarket/storage.py tests/unit/test_storage.py
git commit -m "feat: persist replayable opportunity evidence"
```

## Task 9: Polymarket Gamma and CLOB REST adapters

**Files:**
- Create: `predmarket/polymarket/__init__.py`
- Create: `predmarket/polymarket/gamma.py`
- Create: `predmarket/polymarket/clob.py`
- Create: `tests/fixtures/gamma_markets_page.json`
- Create: `tests/fixtures/clob_books.json`
- Create: `tests/fixtures/clob_fee.json`
- Create: `tests/integration/test_rest_adapters.py`

- [ ] **Step 1: Freeze representative official-shaped fixtures**

Create `tests/fixtures/clob_books.json`:

```json
[
  {
    "market": "condition-1",
    "asset_id": "yes-1",
    "timestamp": "1760000000000",
    "hash": "hash-yes",
    "bids": [{"price": "0.48", "size": "100"}],
    "asks": [{"price": "0.49", "size": "100"}],
    "min_order_size": "1",
    "tick_size": "0.01",
    "neg_risk": false,
    "last_trade_price": "0.48"
  }
]
```

Create `tests/fixtures/clob_fee.json`:

```json
{"base_fee": 500, "exponent": 1, "taker_only": true}
```

Create `tests/fixtures/gamma_markets_page.json`:

```json
{
  "markets": [{
    "id": "1",
    "conditionId": "condition-1",
    "question": "Example?",
    "clobTokenIds": "[\"yes-1\",\"no-1\"]",
    "outcomes": "[\"Yes\",\"No\"]",
    "enableOrderBook": true,
    "negRisk": false,
    "active": true,
    "closed": false
  }],
  "next_cursor": ""
}
```

- [ ] **Step 2: Write failing mocked transport tests**

```python
# tests/integration/test_rest_adapters.py
import json
from pathlib import Path
import httpx
import pytest
from predmarket.polymarket.clob import ClobRestClient
from predmarket.polymarket.gamma import GammaClient


def fixture(name):
    return json.loads(Path(f"tests/fixtures/{name}").read_text())


@pytest.mark.asyncio
async def test_gamma_keyset_and_clob_books_parse_exact_decimals():
    def handler(request):
        if request.url.host == "gamma.test":
            return httpx.Response(200, json=fixture("gamma_markets_page.json"))
        if request.url.path == "/books":
            return httpx.Response(200, json=fixture("clob_books.json"))
        return httpx.Response(200, json=fixture("clob_fee.json"))
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        markets = await GammaClient(http, "https://gamma.test").active_markets()
        books = await ClobRestClient(http, "https://clob.test").books(["yes-1"])
    assert markets[0].yes_token_id == "yes-1"
    assert str(books["yes-1"].asks[0].price) == "0.49"
```

- [ ] **Step 3: Verify RED**

Run:

```bash
pytest tests/integration/test_rest_adapters.py -v
```

Expected: missing Polymarket adapter modules.

- [ ] **Step 4: Implement strict adapters and reject unknown shapes**

```python
# predmarket/polymarket/gamma.py
from dataclasses import dataclass
import json
import httpx


@dataclass(frozen=True)
class MarketMetadata:
    market_id: str
    condition_id: str
    question: str
    yes_token_id: str
    no_token_id: str
    neg_risk: bool


class GammaClient:
    def __init__(self, http: httpx.AsyncClient, base_url: str):
        self.http, self.base_url = http, base_url

    async def active_markets(self) -> list[MarketMetadata]:
        cursor = ""
        result: list[MarketMetadata] = []
        while True:
            response = await self.http.get(f"{self.base_url}/markets/keyset", params={"after_cursor": cursor})
            response.raise_for_status()
            payload = response.json()
            for raw in payload["markets"]:
                tokens = json.loads(raw["clobTokenIds"])
                outcomes = json.loads(raw["outcomes"])
                if outcomes != ["Yes", "No"] or len(tokens) != 2:
                    raise ValueError("unexpected binary market shape")
                result.append(MarketMetadata(str(raw["id"]), raw["conditionId"], raw["question"], tokens[0], tokens[1], bool(raw["negRisk"])))
            cursor = payload.get("next_cursor", "")
            if not cursor:
                return result
```

```python
# predmarket/polymarket/clob.py
from decimal import Decimal
import httpx
from predmarket.domain import BookLevel
from predmarket.fees import FeeSchedule
from predmarket.orderbook import OrderBook


class ClobRestClient:
    def __init__(self, http: httpx.AsyncClient, base_url: str):
        self.http, self.base_url = http, base_url

    async def books(self, token_ids: list[str]) -> dict[str, OrderBook]:
        response = await self.http.post(f"{self.base_url}/books", json=[{"token_id": x} for x in token_ids])
        response.raise_for_status()
        result = {}
        for raw in response.json():
            token = raw["asset_id"]
            result[token] = OrderBook(
                token_id=token,
                bids=tuple(BookLevel(Decimal(x["price"]), Decimal(x["size"])) for x in raw["bids"]),
                asks=tuple(BookLevel(Decimal(x["price"]), Decimal(x["size"])) for x in raw["asks"]),
                tick_size=Decimal(raw["tick_size"]),
                minimum_order_size=Decimal(raw["min_order_size"]),
                exchange_ts_ms=int(raw["timestamp"]),
                book_hash=raw["hash"],
            )
        if set(result) != set(token_ids):
            raise ValueError("batch response omitted a requested token")
        return result

    async def fee_schedules(self, token_ids: list[str], captured_at_ms: int) -> dict[str, FeeSchedule]:
        result: dict[str, FeeSchedule] = {}
        for token_id in token_ids:
            response = await self.http.get(f"{self.base_url}/fee-rate", params={"token_id": token_id})
            response.raise_for_status()
            raw = response.json()
            result[token_id] = FeeSchedule(
                rate=Decimal(raw["base_fee"]) / Decimal("10000"),
                exponent=int(raw.get("exponent", 1)),
                taker_only=bool(raw.get("taker_only", True)),
                captured_at_ms=captured_at_ms,
            )
        return result
```

- [ ] **Step 5: Run contract tests and commit**

Run:

```bash
pytest tests/integration/test_rest_adapters.py -v
```

Expected: `1 passed`.

Commit:

```bash
git add predmarket/polymarket tests/fixtures tests/integration/test_rest_adapters.py
git commit -m "feat: integrate public Polymarket REST data"
```

## Task 10: Candidate detection, REST reconfirmation, and evidence pipeline

**Files:**
- Create: `predmarket/engine.py`
- Create: `tests/integration/test_engine.py`

- [ ] **Step 1: Write a failing two-stage confirmation test**

```python
# tests/integration/test_engine.py
from decimal import Decimal
import pytest
from predmarket.engine import ScannerEngine


@pytest.mark.asyncio
async def test_candidate_that_disappears_is_recorded_but_not_notified(fake_services):
    fake_services.discovery_books.set_binary(yes_ask=".49", no_ask=".48")
    fake_services.confirmation_books.set_binary(yes_ask=".52", no_ask=".50")
    engine = ScannerEngine(fake_services.dependencies())
    result = await engine.evaluate_binary("yes", "no")
    assert result.status == "expired_before_confirmation"
    assert fake_services.store.saved_count == 1
    assert fake_services.notifier.calls == []
```

Create the complete fake in the same file:

```python
class FakeBooks:
    def __init__(self):
        self.values = {}
    def set_binary(self, yes_ask, no_ask):
        self.values = {"yes": yes_ask, "no": no_ask}
    async def books(self, token_ids):
        from decimal import Decimal
        from predmarket.domain import BookLevel
        from predmarket.orderbook import OrderBook
        return {
            token: OrderBook(
                token, (BookLevel(Decimal(self.values[token]) - Decimal(".01"), Decimal("100")),),
                (BookLevel(Decimal(self.values[token]), Decimal("100")),),
                Decimal(".01"), Decimal("1"), 1_000, f"hash-{token}",
            )
            for token in token_ids
        }


class FakeStore:
    def __init__(self):
        self.saved_count = 0
        self.last_bundle = None
    async def save_opportunity(self, bundle):
        self.saved_count += 1
        self.last_bundle = bundle
        return "id-1"


class FakeNotifier:
    def __init__(self):
        self.calls = []
    async def notify(self, opportunity_id, bundle):
        self.calls.append((opportunity_id, bundle))


class FakeServices:
    def __init__(self):
        from types import SimpleNamespace
        self.discovery_books = FakeBooks()
        self.confirmation_books = FakeBooks()
        self.store = FakeStore()
        self.notifier = FakeNotifier()
        self.settings = SimpleNamespace(
            minimum_return=Decimal(".0075"),
            safety_buffer_rate=Decimal(".0025"),
            default_simulation_quantity=Decimal("10"),
            conversion_cost=Decimal("0"),
            max_leg_failure_loss=Decimal("5"),
            max_unhedged_notional=Decimal("20"),
        )

    def dependencies(self):
        from predmarket.engine import EngineDependencies
        return EngineDependencies(
            discovery=self.discovery_books,
            confirmation=self.confirmation_books,
            fee_provider=None,
            store=self.store,
            notifier=self.notifier,
            settings=self.settings,
        )


@pytest.fixture
def fake_services():
    services = FakeServices()
    services.discovery_books.set_binary(".49", ".48")
    services.confirmation_books.set_binary(".52", ".50")
    return services
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/integration/test_engine.py -v
```

Expected: import error for `predmarket.engine`.

- [ ] **Step 3: Implement explicit pipeline dependencies and result states**

```python
# predmarket/engine.py
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from predmarket.actions import binary_underpriced_path
from predmarket.domain import OpportunityStatus
from predmarket.simulator import simulate_path


class BookProvider(Protocol):
    async def books(self, token_ids: list[str]): ...


@dataclass(frozen=True)
class EngineDependencies:
    discovery: BookProvider
    confirmation: BookProvider
    fee_provider: object
    store: object
    notifier: object
    settings: object


@dataclass(frozen=True)
class EngineResult:
    status: str
    opportunity_id: str


class ScannerEngine:
    def __init__(self, deps: EngineDependencies):
        self.deps = deps

    async def evaluate_binary(self, yes: str, no: str) -> EngineResult:
        token_ids = [yes, no]
        discovery = await self.deps.discovery.books(token_ids)
        if not self._candidate(discovery, yes, no):
            bundle = {"risk": {"status": OpportunityStatus.REJECTED.value}, "engine_version": "0.2.0", "reason": "no_candidate"}
            return EngineResult("no_candidate", await self.deps.store.save_opportunity(bundle))
        confirmed = await self.deps.confirmation.books(token_ids)
        if not self._candidate(confirmed, yes, no):
            bundle = {"risk": {"status": OpportunityStatus.REJECTED.value}, "engine_version": "0.2.0", "reason": "expired_before_confirmation"}
            return EngineResult("expired_before_confirmation", await self.deps.store.save_opportunity(bundle))
        bundle = {"risk": {"status": OpportunityStatus.SNAPSHOT_EXECUTABLE.value}, "engine_version": "0.2.0", "books": [yes, no]}
        opportunity_id = await self.deps.store.save_opportunity(bundle)
        await self.deps.notifier.notify(opportunity_id, bundle)
        return EngineResult("SNAPSHOT_EXECUTABLE", opportunity_id)

    @staticmethod
    def _candidate(books, yes: str, no: str) -> bool:
        one = Decimal("1")
        return books[yes].asks[0].price + books[no].asks[0].price < one
```

- [ ] **Step 4: Add simulator, latency, and risk gates**

Add this test and extend `FakeServices` so `last_bundle` stores the submitted evidence:

```python
@pytest.mark.asyncio
async def test_notification_requires_depth_fee_latency_and_risk_gates(fake_services):
    fake_services.configure_profitable_confirmed_depth()
    result = await ScannerEngine(fake_services.dependencies()).evaluate_binary("yes", "no")
    assert result.status == "SNAPSHOT_EXECUTABLE"
    saved = fake_services.store.last_bundle
    assert saved["economics"]["minimum_return"] >= "0.0075"
    assert saved["risk"]["worst_leg_failure_loss"] <= "5"
    assert set(saved) >= {"books", "fee_schedules", "actions", "risk", "economics", "timings"}
```

Replace the confirmed branch in `ScannerEngine.evaluate_binary` with:

```python
path = binary_underpriced_path(yes, no)
fee_schedules = await self.deps.confirmation.fee_schedules(
    token_ids, captured_at_ms=self.deps.clock.wall_ms()
)
simulation = simulate_path(
    path=path,
    books=confirmed,
    fees=fee_schedules,
    quantity=self.deps.settings.default_simulation_quantity,
    safety_buffer_rate=self.deps.settings.safety_buffer_rate,
    conversion_cost=self.deps.settings.conversion_cost,
)
timing = self.deps.validate_confirmed_timings(confirmed)
partial = self.deps.partial_fill_model(confirmed, path, simulation)
assessment = self.deps.assess(
    simulation=simulation,
    timing=timing,
    partial=partial,
    minimum_return=self.deps.settings.minimum_return,
    max_loss=self.deps.settings.max_leg_failure_loss,
    max_unhedged=self.deps.settings.max_unhedged_notional,
)
bundle = self.deps.serialize_evidence(
    path=path,
    books=confirmed,
    fee_schedules=fee_schedules,
    simulation=simulation,
    timing=timing,
    partial=partial,
    assessment=assessment,
)
opportunity_id = await self.deps.store.save_opportunity(bundle)
if assessment.status is OpportunityStatus.SNAPSHOT_EXECUTABLE:
    await self.deps.notifier.notify(opportunity_id, bundle)
return EngineResult(assessment.status.value, opportunity_id)
```

Add these exact members to `EngineDependencies`:

```python
    clock: object
    validate_confirmed_timings: object
    partial_fill_model: object
    assess: object
    serialize_evidence: object
```

Delete `fee_provider`; the confirming `ClobRestClient` owns both `books()` and
`fee_schedules()`. `serialize_evidence` must convert every `Decimal` to a string
and include every confirmed bid/ask level, not only token IDs.

- [ ] **Step 5: Run integration tests and commit**

Run:

```bash
pytest tests/integration/test_engine.py -v
```

Expected: `2 passed`.

Commit:

```bash
git add predmarket/engine.py tests/integration/test_engine.py
git commit -m "feat: confirm and risk-check candidate opportunities"
```

## Task 11: WebSocket receiver, bounded queue, and resynchronization

**Files:**
- Create: `predmarket/polymarket/ws.py`
- Create: `tests/fixtures/ws_book.json`
- Create: `tests/fixtures/ws_price_change.json`
- Create: `tests/integration/test_websocket.py`

- [ ] **Step 1: Add frozen WebSocket messages and failing overflow test**

Create `tests/fixtures/ws_book.json`:

```json
{
  "event_type": "book",
  "asset_id": "yes-1",
  "market": "condition-1",
  "bids": [{"price": "0.48", "size": "100"}],
  "asks": [{"price": "0.49", "size": "100"}],
  "timestamp": "1760000000000",
  "hash": "h1"
}
```

```python
# tests/integration/test_websocket.py
import asyncio
import pytest
from predmarket.epochs import EpochBook, EpochState
from predmarket.polymarket.ws import MarketMessageReceiver


@pytest.mark.asyncio
async def test_queue_overflow_invalidates_affected_epoch():
    queue = asyncio.Queue(maxsize=1)
    epochs = {"yes-1": EpochBook("yes-1")}
    receiver = MarketMessageReceiver(queue, epochs)
    await receiver.accept({"event_type": "book", "asset_id": "yes-1", "timestamp": "1"})
    await receiver.accept({"event_type": "price_change", "price_changes": [{"asset_id": "yes-1"}], "timestamp": "2"})
    assert epochs["yes-1"].state is EpochState.RESYNC
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/integration/test_websocket.py -v
```

Expected: missing `predmarket.polymarket.ws`.

- [ ] **Step 3: Implement a receive-only bounded-queue adapter**

```python
# predmarket/polymarket/ws.py
import asyncio
import time
from dataclasses import dataclass
from predmarket.epochs import EpochBook


@dataclass(frozen=True)
class ReceivedMessage:
    payload: dict
    received_ts_ms: int
    received_monotonic: float


class MarketMessageReceiver:
    def __init__(self, queue: asyncio.Queue, epochs: dict[str, EpochBook]):
        self.queue, self.epochs = queue, epochs

    async def accept(self, payload: dict) -> None:
        message = ReceivedMessage(payload, int(time.time() * 1000), time.monotonic())
        try:
            self.queue.put_nowait(message)
        except asyncio.QueueFull:
            assets = [payload.get("asset_id")] + [x.get("asset_id") for x in payload.get("price_changes", [])]
            for asset in filter(None, assets):
                if asset in self.epochs:
                    self.epochs[asset].invalidate("queue_overflow")
```

- [ ] **Step 4: Add reconnect and full-snapshot recovery integration test**

```python
# append to tests/integration/test_websocket.py
@pytest.mark.asyncio
async def test_disconnect_discards_deltas_until_full_snapshot():
    epoch = EpochBook("yes-1")
    epoch.replace_snapshot("h1", 1)
    epoch.invalidate("disconnect")
    epoch.apply_delta(".50", "10", "BUY", 2)
    assert epoch.state is EpochState.RESYNC
    epoch.replace_snapshot("h2", 3)
    assert epoch.state is EpochState.LIVE
    assert epoch.snapshot_hash == "h2"
```

Run:

```bash
pytest tests/integration/test_websocket.py tests/unit/test_epochs.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add predmarket/polymarket/ws.py tests/fixtures/ws_book.json tests/fixtures/ws_price_change.json tests/integration/test_websocket.py
git commit -m "feat: receive live books with fail-closed resync"
```

## Task 12: CLI, notifications, reports, and deterministic replay

**Files:**
- Replace: `predmarket/cli.py`
- Replace: `predmarket/__main__.py`
- Create: `predmarket/commands.py`
- Create: `predmarket/runtime.py`
- Create: `predmarket/notifier.py`
- Create: `tests/integration/test_cli.py`
- Modify: `README.md`

- [ ] **Step 1: Write failing CLI help and notification-routing tests**

```python
# tests/integration/test_cli.py
from predmarket.cli import build_parser


def test_cli_exposes_approved_commands():
    parser = build_parser()
    help_text = parser.format_help()
    for command in ("sync-markets", "scan-once", "watch", "relations", "replay", "report"):
        assert command in help_text
```

```python
# append to tests/integration/test_cli.py
import pytest
from predmarket.domain import OpportunityStatus
from predmarket.notifier import NotificationRouter


class FakeDesktop:
    def __init__(self):
        self.ids = []
    async def send(self, opportunity_id, bundle):
        self.ids.append(opportunity_id)


@pytest.fixture
def fake_desktop():
    return FakeDesktop()


@pytest.mark.asyncio
async def test_only_snapshot_executable_reaches_desktop(fake_desktop):
    router = NotificationRouter(fake_desktop)
    await router.notify("1", {"risk": {"status": OpportunityStatus.RESEARCH_CANDIDATE.value}})
    await router.notify("2", {"risk": {"status": OpportunityStatus.SNAPSHOT_EXECUTABLE.value}})
    assert fake_desktop.ids == ["2"]
```

- [ ] **Step 2: Verify RED**

Run:

```bash
pytest tests/integration/test_cli.py -v
```

Expected: missing `build_parser` or `predmarket.notifier`.

- [ ] **Step 3: Implement the command surface**

```python
# predmarket/cli.py
import argparse
import asyncio


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="predmarket")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("sync-markets")
    commands.add_parser("scan-once")
    commands.add_parser("watch")
    relations = commands.add_parser("relations")
    relation_commands = relations.add_subparsers(dest="relations_command", required=True)
    relation_commands.add_parser("list")
    relation_commands.add_parser("validate").add_argument("path")
    relation_commands.add_parser("import").add_argument("path")
    commands.add_parser("replay").add_argument("opportunity_id")
    commands.add_parser("report")
    return parser


async def run(args: argparse.Namespace) -> int:
    from predmarket.commands import dispatch
    return await dispatch(args)


def main() -> int:
    return asyncio.run(run(build_parser().parse_args()))
```

```python
# predmarket/__main__.py
from predmarket.cli import main

raise SystemExit(main())
```

Create the dependency-wiring module with one explicit branch per command:

```python
# predmarket/commands.py
import json
from pathlib import Path
from predmarket.relations import load_relation


async def dispatch(args) -> int:
    if args.command == "relations" and args.relations_command == "validate":
        relation = load_relation(Path(args.path))
        print(json.dumps({"id": relation.relation_id, "version": relation.version, "valid": True}))
        return 0
    if args.command == "relations" and args.relations_command in {"list", "import"}:
        raise SystemExit("relation persistence is not configured")
    if args.command in {"sync-markets", "scan-once", "watch", "replay", "report"}:
        from predmarket.runtime import build_runtime
        runtime = await build_runtime()
        return await getattr(runtime, args.command.replace("-", "_"))(args)
    raise SystemExit(f"unsupported command: {args.command}")
```

Add `predmarket/runtime.py` to Task 12 files. It must create `Settings`, one shared
`httpx.AsyncClient`, `GammaClient`, `ClobRestClient`, `Store`,
`NotificationRouter`, and `ScannerEngine`, and expose methods named
`sync_markets`, `scan_once`, `watch`, `replay`, and `report`. No runtime method may
construct or import an authenticated trading client.

- [ ] **Step 4: Implement side-effect-isolated notification routing**

```python
# predmarket/notifier.py
import asyncio
from predmarket.domain import OpportunityStatus


class NotificationRouter:
    def __init__(self, desktop):
        self.desktop = desktop

    async def notify(self, opportunity_id: str, bundle: dict) -> None:
        print(f"{bundle['risk']['status']} opportunity={opportunity_id}")
        if bundle["risk"]["status"] != OpportunityStatus.SNAPSHOT_EXECUTABLE.value:
            return
        try:
            await self.desktop.send(opportunity_id, bundle)
        except Exception as exc:
            print(f"desktop_notification_failed: {exc}")


class MacOSDesktop:
    async def send(self, opportunity_id: str, bundle: dict) -> None:
        message = f"Polymarket opportunity {opportunity_id}"
        process = await asyncio.create_subprocess_exec(
            "osascript", "-e", f'display notification "{message}" with title "Prediction Market"',
        )
        await process.wait()
        if process.returncode:
            raise RuntimeError(f"osascript exited {process.returncode}")
```

Update `README.md` with the exact setup and read-only guarantees:

```markdown
## Setup

python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
pytest -q

## Read-only commands

predmarket sync-markets
predmarket scan-once
predmarket watch
predmarket replay OPPORTUNITY_ID
predmarket report

The project contains no authenticated trading client and never reads a private key.
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/integration/test_cli.py -v
predmarket --help
```

Expected: tests pass and help lists all approved commands.

Commit:

```bash
git add predmarket/cli.py predmarket/__main__.py predmarket/commands.py predmarket/runtime.py predmarket/notifier.py tests/integration/test_cli.py README.md
git commit -m "feat: expose read-only scanner commands and alerts"
```

## Task 13: Full verification, live soak commands, and legacy removal

**Files:**
- Delete: `predmarket/core.py`
- Delete: `predmarket/api.py`
- Delete: `predmarket/ledger.py`
- Delete: `tests/test_core.py`
- Modify: `STRATEGY.md`
- Create: `docs/operations/soak-test.md`

- [ ] **Step 1: Run the complete test suite before removing legacy code**

Run:

```bash
pytest -q
```

Expected: all tests pass. Do not delete legacy files if any new module still imports them:

```bash
rg "predmarket\\.(core|api|ledger)|from \\.?(core|api|ledger)" predmarket tests
```

Expected: no output except the legacy files themselves.

- [ ] **Step 2: Remove superseded legacy modules and rerun**

Delete exactly:

```text
predmarket/core.py
predmarket/api.py
predmarket/ledger.py
tests/test_core.py
```

Run:

```bash
pytest -q
python -m predmarket --help
```

Expected: all tests pass and CLI help renders.

- [ ] **Step 3: Add a reproducible 24-hour soak procedure**

```markdown
# docs/operations/soak-test.md

# Scanner soak test

1. Start with a clean database:
   `predmarket sync-markets`
2. Run:
   `predmarket watch 2>&1 | tee data/soak.log`
3. Leave the process running for at least 24 continuous hours.
4. Generate:
   `predmarket report`
5. Acceptance requires:
   - no unhandled exception;
   - every disconnect followed by a full epoch rebuild;
   - zero formal opportunities from stale or overflowed books;
   - complete evidence for every `SNAPSHOT_EXECUTABLE`;
   - latency p50/p95/p99 and queue high-water mark present.
6. Continue daily observation until seven calendar days of data exist.
   Zero opportunities is an acceptable result.
```

- [ ] **Step 4: Update strategy status without claiming profit**

Replace the implementation-status section in `STRATEGY.md` with:

```markdown
## Current validation status

The scanner is read-only. It measures snapshot-executable structural opportunities
after fees, conversion costs, depth, latency, and partial-fill risk. It does not
claim profitability until the 24-hour soak and seven-day observation complete.
```

- [ ] **Step 5: Run final verification and commit**

Run:

```bash
pytest -q
git diff --check
predmarket relations validate rules/example-implication.yaml
predmarket scan-once
predmarket report
```

Expected:

- all automated tests pass;
- whitespace check is clean;
- relation validation succeeds;
- `scan-once` exits successfully even when it finds zero opportunities;
- report includes run duration, stale books, epoch failures, queue high-water mark,
  confirmation expiries, opportunity counts, worst leg failure, and latency percentiles.

Commit:

```bash
git add -A predmarket tests STRATEGY.md docs/operations/soak-test.md
git commit -m "test: verify scanner and document soak validation"
```

## Plan Completion Gate

## Spec Coverage Matrix

| Spec requirement | Implementing task |
|---|---|
| Exact Decimal configuration and $1,000 limits | Task 1 |
| Executable bid/ask depth and dynamic fee schedules | Tasks 2 and 9 |
| Human-certified unit-weight logical rules | Task 3 |
| BUY/SELL/SPLIT/MERGE action paths | Task 4 |
| Neg-risk conversion and hold-to-resolution proceeds | Task 5 |
| $5 leg-loss and $20 unhedged hard gates | Task 6 |
| Four timestamps, 1s age, 250ms skew, 100ms processing budget | Task 7 |
| Fail-closed book epochs and queue overflow | Tasks 7 and 11 |
| SQLite evidence, status, latency, and replay data | Task 8 |
| Gamma keyset discovery and CLOB batch confirmation | Task 9 |
| WebSocket discovery followed by REST reconfirmation | Tasks 10 and 11 |
| REJECTED / RESEARCH_CANDIDATE / SNAPSHOT_EXECUTABLE | Tasks 6 and 10 |
| Terminal plus macOS notifications after persistence | Task 12 |
| CLI command surface and read-only runtime | Task 12 |
| Property, contract, reconnect, and replay tests | Tasks 2, 7–11 |
| 24-hour soak and seven-day observation | Task 13 |
| No authenticated client, private key, or real order path | Tasks 12 and 13 |

Before calling implementation complete:

```bash
pytest -q
git diff --check
git status --short
```

The first two commands must succeed. `git status --short` must show no implementation
files left uncommitted. The 24-hour soak and seven-day observation remain explicit
operational acceptance work; do not claim those durations were completed from unit tests.

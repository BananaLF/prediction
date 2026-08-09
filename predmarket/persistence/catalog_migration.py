"""Crash-recoverable, side-by-side catalog migration from schema v3 to v4."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
from typing import Any
from uuid import uuid4

from predmarket.persistence.repositories import CatalogRepository
from predmarket.persistence.schema import create_v4_database
from predmarket.persistence.wal import WalPolicy
from predmarket.persistence.writer import DatabaseWriter


_CATALOG_TABLES = ("events", "markets", "tokens")
_DOWNSTREAM_TABLES = (
    "relations",
    "arbitrage_signals",
    "signal_revisions",
    "signal_legs",
    "orderbook_snapshots",
    "orderbook_levels",
    "system_events",
)
_BATCH_SIZE = 500


class MigrationStage(StrEnum):
    BUILDING = "BUILDING"
    VALIDATED = "VALIDATED"
    V3_BACKED_UP = "V3_BACKED_UP"
    V4_INSTALLED = "V4_INSTALLED"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class MigrationResult:
    database: Path
    backup: Path
    source_version: int
    target_version: int


@dataclass(frozen=True, slots=True)
class _Marker:
    database: Path
    temporary: Path
    backup: Path
    stage: MigrationStage
    source_version: int = 3
    target_version: int = 4

    def with_stage(self, stage: MigrationStage) -> _Marker:
        return _Marker(
            database=self.database,
            temporary=self.temporary,
            backup=self.backup,
            stage=stage,
            source_version=self.source_version,
            target_version=self.target_version,
        )


def migration_marker_path(database_path: Path) -> Path:
    path = Path(database_path)
    return path.with_name(f".{path.name}.v4-migration.json")


def migration_lock_path(database_path: Path) -> Path:
    path = Path(database_path)
    return path.with_name(f".{path.name}.v4-migration.lock")


def migrate_v3_to_v4(database_path: Path) -> MigrationResult:
    """Build, validate, and atomically install a side-by-side v4 database."""

    database = Path(database_path)
    with _exclusive_lock(database):
        marker_path = migration_marker_path(database)
        if marker_path.exists():
            raise RuntimeError(
                f"unresolved migration marker exists: {marker_path}; run recovery first"
            )
        _validate_schema_version(database, 3)
        _check_free_space(database)
        _checkpoint_source(database)

        marker = _new_marker(database)
        _write_marker(marker)
        _migration_stage_hook(MigrationStage.BUILDING)
        try:
            _build_v4(marker.database, marker.temporary)
            _validate_copy(marker.database, marker.temporary)
        except BaseException:
            _remove_sqlite_files(marker.temporary)
            _remove_marker(marker.database)
            raise

        marker = marker.with_stage(MigrationStage.VALIDATED)
        _write_marker(marker)
        _migration_stage_hook(MigrationStage.VALIDATED)
        marker = _install_validated_v4(marker)
        return MigrationResult(
            database=marker.database,
            backup=marker.backup,
            source_version=marker.source_version,
            target_version=marker.target_version,
        )


def recover_v4_switch(database_path: Path) -> None:
    """Resolve an interrupted marker-governed v4 file switch."""

    database = Path(database_path)
    with _exclusive_lock(database):
        marker_path = migration_marker_path(database)
        if not marker_path.exists():
            return
        marker = _read_marker(database)
        if marker.database != database.absolute():
            raise RuntimeError("migration marker database path does not match request")

        if marker.stage is MigrationStage.BUILDING:
            _validate_schema_version(marker.database, 3)
            _remove_sqlite_files(marker.temporary)
            _remove_marker(marker.database)
            return
        if marker.stage is MigrationStage.COMPLETE:
            _validate_v4(marker.database)
            return
        _install_validated_v4(marker)


def _migration_stage_hook(stage: MigrationStage) -> None:
    """Test seam used to simulate a process crash after a durable marker write."""


def _new_marker(database: Path) -> _Marker:
    absolute = database.absolute()
    token = uuid4().hex
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    stem = database.name.removesuffix(database.suffix)
    temporary = database.with_name(f".{database.name}.v4-building-{token}.sqlite3")
    backup = database.with_name(f"{stem}.{timestamp}.pre-v4.sqlite3")
    if backup.exists():
        raise FileExistsError(f"automatic migration backup already exists: {backup}")
    return _Marker(
        database=absolute,
        temporary=temporary.absolute(),
        backup=backup.absolute(),
        stage=MigrationStage.BUILDING,
    )


@contextmanager
def _exclusive_lock(database: Path) -> Iterator[None]:
    lock_path = migration_lock_path(database)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"could not acquire migration lock: {lock_path}") from error
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _available_bytes(directory: Path) -> int:
    return int(shutil.disk_usage(directory).free)


def _check_free_space(database: Path) -> None:
    if not database.exists() or database.stat().st_size == 0:
        raise ValueError(f"database does not exist or is empty: {database}")
    source_bytes = database.stat().st_size
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{database}{suffix}")
        if sidecar.exists():
            source_bytes += sidecar.stat().st_size
    policy = WalPolicy()
    estimated_v4_bytes = max(database.stat().st_size, 1024 * 1024)
    required = source_bytes + estimated_v4_bytes + policy.reserve_bytes
    available = _available_bytes(database.parent)
    if available < required:
        raise OSError(
            f"insufficient free space for v4 migration: need {required}, have {available}"
        )


def _validate_schema_version(path: Path, expected: int) -> None:
    if not path.exists() or path.stat().st_size == 0:
        raise ValueError(f"database does not exist or is empty: {path}")
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != expected:
            raise ValueError(
                f"unsupported database schema version {version}; expected {expected}"
            )
        _check_integrity(connection)
        if expected == 3:
            tables = _object_names(connection, "table")
            required = set(_CATALOG_TABLES + _DOWNSTREAM_TABLES)
            if not required <= tables:
                raise ValueError("database does not have the expected schema v3 tables")


def _checkpoint_source(path: Path) -> None:
    with sqlite3.connect(path, isolation_level=None) as connection:
        connection.execute("PRAGMA busy_timeout = 0")
        result = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if result is not None and int(result[0]) != 0:
            raise RuntimeError("source WAL is busy; stop all database writers and retry")


def _build_v4(source_path: Path, target_path: Path) -> None:
    create_v4_database(target_path)
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    target = sqlite3.connect(target_path)
    target.execute("PRAGMA foreign_keys = ON")
    try:
        generation_id = int(
            target.execute(
                "SELECT active_generation_id FROM catalog_state WHERE id = 1"
            ).fetchone()[0]
        )
        sync_generation, input_digest = _baseline_identity(source)
        catalog_counts = {
            table: int(source.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in _CATALOG_TABLES
        }
        target.execute(
            """
            UPDATE catalog_generations
            SET sync_generation = ?, input_digest = ?,
                planned_event_count = ?, planned_market_count = ?, planned_token_count = ?,
                written_event_count = ?, written_market_count = ?, written_token_count = ?,
                candidate_event_count = ?, candidate_market_count = ?, candidate_token_count = ?,
                candidate_snapshot_digest = ?
            WHERE id = ?
            """,
            (
                sync_generation,
                input_digest,
                catalog_counts["events"],
                catalog_counts["markets"],
                catalog_counts["tokens"],
                catalog_counts["events"],
                catalog_counts["markets"],
                catalog_counts["tokens"],
                catalog_counts["events"],
                catalog_counts["markets"],
                catalog_counts["tokens"],
                input_digest,
                generation_id,
            ),
        )
        target.commit()

        _copy_catalog(
            source,
            target,
            "events",
            "catalog_event_ids",
            "event_versions",
            generation_id,
        )
        _copy_catalog(
            source,
            target,
            "markets",
            "catalog_market_ids",
            "market_versions",
            generation_id,
        )
        _copy_catalog(
            source,
            target,
            "tokens",
            "catalog_token_ids",
            "token_versions",
            generation_id,
        )
        for table in _DOWNSTREAM_TABLES:
            _copy_downstream(source, target, table)
        target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        target.close()
        source.close()


def _baseline_identity(source: sqlite3.Connection) -> tuple[str, str]:
    digest = _database_digest(source, _CATALOG_TABLES)
    generations = {
        str(row[0])
        for table in _CATALOG_TABLES
        for row in source.execute(f'SELECT DISTINCT sync_generation FROM "{table}"')
    }
    sync_generation = (
        next(iter(generations))
        if len(generations) == 1
        else f"migration-v3-{digest[:24]}"
    )
    return sync_generation, digest


def _copy_catalog(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    source_table: str,
    identity_table: str,
    version_table: str,
    generation_id: int,
) -> None:
    source_columns = _columns(source, source_table)
    version_columns = [
        column
        for column in _columns(target, version_table)
        if column not in {"entity_id", "generation_id"}
    ]
    payload_columns = [column for column in version_columns if column in source_columns]
    if source_table == "events":
        identity_columns = ("id", "created_at")
    elif source_table == "markets":
        identity_columns = ("id", "event_id", "created_at")
    else:
        identity_columns = ("id", "market_id", "created_at")
    selected = list(dict.fromkeys((*identity_columns, *payload_columns)))

    def insert(row: sqlite3.Row) -> None:
        target.execute(
            f'INSERT INTO "{identity_table}" ({_quoted(identity_columns)}) '
            f'VALUES ({_placeholders(identity_columns)})',
            tuple(row[column] for column in identity_columns),
        )
        version_insert_columns = ("entity_id", "generation_id", *payload_columns)
        target.execute(
            f'INSERT INTO "{version_table}" ({_quoted(version_insert_columns)}) '
            f'VALUES ({_placeholders(version_insert_columns)})',
            (row["id"], generation_id, *(row[column] for column in payload_columns)),
        )

    _copy_keyset_batches(source, target, source_table, selected, insert)


def _copy_downstream(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    table: str,
) -> None:
    columns = _columns(source, table)
    if columns != _columns(target, table):
        raise ValueError(f"v3/v4 column mismatch for {table}")
    sql = (
        f'INSERT INTO "{table}" ({_quoted(columns)}) '
        f'VALUES ({_placeholders(columns)})'
    )
    _copy_keyset_batches(
        source,
        target,
        table,
        columns,
        lambda row: target.execute(sql, tuple(row[column] for column in columns)),
    )


def _copy_keyset_batches(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    insert: Callable[[sqlite3.Row], Any],
) -> None:
    last_rowid = 0
    policy = WalPolicy()
    while True:
        rows = source.execute(
            f'SELECT rowid AS "__migration_rowid", {_quoted(columns)} '
            f'FROM "{table}" WHERE rowid > ? ORDER BY rowid LIMIT ?',
            (last_rowid, _BATCH_SIZE),
        ).fetchall()
        if not rows:
            return
        try:
            target.execute("BEGIN")
            for row in rows:
                insert(row)
            target.commit()
        except BaseException:
            target.rollback()
            raise
        last_rowid = int(rows[-1]["__migration_rowid"])
        wal_path = Path(f"{target.execute('PRAGMA database_list').fetchone()[2]}-wal")
        if wal_path.exists() and wal_path.stat().st_size >= policy.preflight_bytes:
            target.execute("PRAGMA wal_checkpoint(PASSIVE)")
        if wal_path.exists() and wal_path.stat().st_size >= policy.hard_limit_bytes:
            raise OSError("migration WAL exceeded its hard size limit")


def _validate_copy(source_path: Path, target_path: Path) -> None:
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    target = sqlite3.connect(f"file:{target_path}?mode=ro", uri=True)
    try:
        _validate_v4_connection(target)
        for table in _CATALOG_TABLES + _DOWNSTREAM_TABLES:
            source_count = int(
                source.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            target_count = int(
                target.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            if source_count != target_count:
                raise ValueError(f"row count mismatch for {table}")
            excluded = (
                {"sync_generation", "sync_generation_complete"}
                if table in _CATALOG_TABLES
                else set()
            )
            if _table_digest(source, table, excluded) != _table_digest(
                target, table, excluded
            ):
                raise ValueError(f"normalized digest mismatch for {table}")
        target.execute("SELECT id, title FROM events ORDER BY CAST(id AS BLOB)").fetchall()
        target.execute(
            """
            SELECT relations.id, markets.id
            FROM relations JOIN markets ON markets.id = relations.market_a_id
            LIMIT 1
            """
        ).fetchall()
        target.execute(
            """
            SELECT signal_legs.signal_id, tokens.id
            FROM signal_legs
            JOIN tokens ON tokens.market_id = signal_legs.market_id
                       AND tokens.id = signal_legs.token_id
            LIMIT 1
            """
        ).fetchall()
        _load_catalog(target_path)
    finally:
        target.close()
        source.close()


def _load_catalog(path: Path) -> None:
    async def load() -> None:
        repository = CatalogRepository(path, DatabaseWriter(path))
        await repository.load_catalog()

    # Keep the synchronous migration API safe when called by an application that
    # already owns an asyncio event loop.
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(asyncio.run, load()).result()


def _validate_v4(path: Path) -> None:
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
        _validate_v4_connection(connection)


def _validate_v4_connection(connection: sqlite3.Connection) -> None:
    version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if version != 4:
        raise ValueError(f"validated migration target has schema version {version}, expected 4")
    _check_integrity(connection)
    active = connection.execute(
        """
        SELECT generations.status
        FROM catalog_state AS state
        JOIN catalog_generations AS generations
          ON generations.id = state.active_generation_id
        WHERE state.id = 1
        """
    ).fetchall()
    if active != [("COMMITTED",)]:
        raise ValueError("v4 active catalog generation is invalid")


def _install_validated_v4(marker: _Marker) -> _Marker:
    if marker.stage is MigrationStage.VALIDATED:
        if marker.database.exists():
            _validate_schema_version(marker.database, 3)
            if marker.backup.exists():
                raise RuntimeError("migration backup path already exists")
            os.replace(marker.database, marker.backup)
            _fsync_directory(marker.database.parent)
        elif not marker.backup.exists():
            raise RuntimeError("validated migration lost both source and backup")
        _validate_schema_version(marker.backup, 3)
        marker = marker.with_stage(MigrationStage.V3_BACKED_UP)
        _write_marker(marker)
        _migration_stage_hook(MigrationStage.V3_BACKED_UP)

    if marker.stage is MigrationStage.V3_BACKED_UP:
        _validate_schema_version(marker.backup, 3)
        if marker.database.exists():
            _validate_v4(marker.database)
        else:
            _validate_v4(marker.temporary)
            os.replace(marker.temporary, marker.database)
            _fsync_directory(marker.database.parent)
        marker = marker.with_stage(MigrationStage.V4_INSTALLED)
        _write_marker(marker)
        _migration_stage_hook(MigrationStage.V4_INSTALLED)

    if marker.stage is MigrationStage.V4_INSTALLED:
        _validate_v4(marker.database)
        marker = marker.with_stage(MigrationStage.COMPLETE)
        _write_marker(marker)
        _migration_stage_hook(MigrationStage.COMPLETE)
    return marker


def _write_marker(marker: _Marker) -> None:
    path = migration_marker_path(marker.database)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    payload = {
        "backup": str(marker.backup),
        "database": str(marker.database),
        "source_version": marker.source_version,
        "stage": marker.stage.value,
        "target_version": marker.target_version,
        "temporary": str(marker.temporary),
    }
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _read_marker(database: Path) -> _Marker:
    path = migration_marker_path(database)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        marker = _Marker(
            database=Path(payload["database"]),
            temporary=Path(payload["temporary"]),
            backup=Path(payload["backup"]),
            stage=MigrationStage(payload["stage"]),
            source_version=int(payload["source_version"]),
            target_version=int(payload["target_version"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(f"invalid migration marker: {path}") from error
    if marker.source_version != 3 or marker.target_version != 4:
        raise RuntimeError("migration marker has unsupported schema versions")
    expected_database = database.absolute()
    if marker.database != expected_database:
        raise RuntimeError("migration marker database path does not match request")
    expected_parent = expected_database.parent
    if (
        marker.temporary.parent != expected_parent
        or marker.backup.parent != expected_parent
        or marker.temporary == expected_database
        or marker.backup == expected_database
    ):
        raise RuntimeError("migration marker paths must remain beside the database")
    return marker


def _remove_marker(database: Path) -> None:
    path = migration_marker_path(database)
    path.unlink(missing_ok=True)
    _fsync_directory(path.parent)


def _remove_sqlite_files(path: Path) -> None:
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        candidate.unlink(missing_ok=True)


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _check_integrity(connection: sqlite3.Connection) -> None:
    integrity = connection.execute("PRAGMA integrity_check").fetchall()
    if integrity != [("ok",)]:
        raise sqlite3.DatabaseError(f"SQLite integrity check failed: {integrity!r}")
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_keys:
        raise sqlite3.DatabaseError(f"foreign key check failed: {foreign_keys!r}")


def _object_names(connection: sqlite3.Connection, object_type: str) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = ? AND name NOT LIKE 'sqlite_%'",
            (object_type,),
        )
    }


def _columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')]


def _quoted(columns: Sequence[str]) -> str:
    return ", ".join('"' + column.replace('"', '""') + '"' for column in columns)


def _placeholders(columns: Sequence[str]) -> str:
    return ", ".join("?" for _ in columns)


def _database_digest(connection: sqlite3.Connection, tables: Sequence[str]) -> str:
    digest = hashlib.sha256()
    for table in tables:
        digest.update(table.encode())
        digest.update(_table_digest(connection, table, set()).encode())
    return digest.hexdigest()


def _table_digest(
    connection: sqlite3.Connection,
    table: str,
    excluded: set[str],
) -> str:
    columns = [column for column in _columns(connection, table) if column not in excluded]
    order = ", ".join(f'CAST("{column}" AS BLOB)' for column in columns)
    rows = connection.execute(
        f'SELECT {_quoted(columns)} FROM "{table}" ORDER BY {order}'
    ).fetchall()
    normalized_rows = [tuple(row) for row in rows]
    encoded = json.dumps(
        normalized_rows,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

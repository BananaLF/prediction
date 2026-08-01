"""Deliberately narrow, operator-only SQLite reset support."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import subprocess

from predmarket.config import AppConfig


class ResetRefused(ValueError):
    """Raised when a requested database reset is not demonstrably safe."""


@dataclass(frozen=True)
class ResetPlan:
    main_path: Path
    targets: tuple[Path, Path, Path]


def prepare_reset(config_path: Path, *, working_directory: Path | None = None) -> ResetPlan:
    """Resolve the configured SQLite path and reject unsafe reset targets."""
    configured = AppConfig.load(config_path).database.path.expanduser()
    cwd = (working_directory or Path.cwd()).resolve()
    candidate = configured if configured.is_absolute() else cwd / configured
    absolute = candidate.absolute()
    resolved = absolute.resolve(strict=False)

    if _contains_symlink(absolute) or absolute.is_symlink():
        raise ResetRefused("configured database path must not be a symlink")
    if absolute.exists() and absolute.is_dir():
        raise ResetRefused("configured database path must be a file, not a directory")

    forbidden = {
        Path("/").resolve(),
        Path.home().resolve(),
        Path(__file__).resolve().parents[1],
    }
    if resolved in forbidden:
        raise ResetRefused("configured database path is a protected root")

    targets = (resolved, Path(f"{resolved}-wal"), Path(f"{resolved}-shm"))
    for target in targets:
        if target.is_symlink():
            raise ResetRefused(f"reset target must not be a symlink: {target}")
        if target.exists() and not target.is_file():
            raise ResetRefused(f"reset target must be a file: {target}")
    return ResetPlan(main_path=resolved, targets=targets)


def running_predmarket_processes() -> tuple[int, ...]:
    """Return other local processes that are running the predmarket executable."""
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            capture_output=True,
            check=False,
            text=True,
        )
    except OSError as error:
        raise ResetRefused(
            "could not verify whether a predmarket process is running"
        ) from error
    if result.returncode != 0:
        raise ResetRefused("could not verify whether a predmarket process is running")

    processes: list[int] = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(maxsplit=1)
        if len(fields) != 2 or not fields[0].isdigit():
            continue
        pid, command = int(fields[0]), fields[1]
        if pid != os.getpid() and _is_predmarket_command(command):
            processes.append(pid)
    return tuple(processes)


def execute_reset(plan: ResetPlan, *, running_processes: tuple[int, ...] | None = None) -> tuple[Path, ...]:
    """Delete only the validated SQLite main file and exact WAL/SHM siblings."""
    active = running_predmarket_processes() if running_processes is None else running_processes
    if active:
        raise ResetRefused(
            "predmarket process is running (pid "
            + ", ".join(str(pid) for pid in active)
            + ")"
        )

    deleted: list[Path] = []
    for target in plan.targets:
        if target.exists():
            target.unlink()
            deleted.append(target)
    return tuple(deleted)


def _contains_symlink(path: Path) -> bool:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            return True
    return False


def _is_predmarket_command(command: str) -> bool:
    words = command.split()
    return "predmarket" in words or (
        "-m" in words
        and words.index("-m") + 1 < len(words)
        and words[words.index("-m") + 1] == "predmarket"
    )

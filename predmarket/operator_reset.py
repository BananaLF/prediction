"""Deliberately narrow, operator-only SQLite reset support."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import shlex
import stat
import subprocess

from predmarket.config import AppConfig


class ResetRefused(ValueError):
    """Raised when a requested database reset is not demonstrably safe."""


@dataclass(frozen=True)
class ResetPlan:
    main_path: Path
    targets: tuple[Path, Path, Path]
    parent_path: Path
    target_names: tuple[str, str, str]
    parent_device: int
    parent_inode: int


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
    parent_path = resolved.parent
    try:
        parent_stat = parent_path.stat(follow_symlinks=False)
    except OSError as error:
        raise ResetRefused("could not verify reset parent directory") from error
    if not stat.S_ISDIR(parent_stat.st_mode):
        raise ResetRefused("reset parent directory must be a directory")
    return ResetPlan(
        main_path=resolved,
        targets=targets,
        parent_path=parent_path,
        target_names=tuple(target.name for target in targets),
        parent_device=parent_stat.st_dev,
        parent_inode=parent_stat.st_ino,
    )


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

    parent_fd = _open_verified_parent(plan)
    try:
        existing_names = _verified_target_names(plan, parent_fd)
        for target, name in zip(plan.targets, existing_names, strict=True):
            if name is not None:
                os.unlink(name, dir_fd=parent_fd)
        return tuple(
            target
            for target, name in zip(plan.targets, existing_names, strict=True)
            if name is not None
        )
    except OSError as error:
        raise ResetRefused("could not safely remove validated reset target") from error
    finally:
        os.close(parent_fd)


def _open_verified_parent(plan: ResetPlan) -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if nofollow is None or directory is None:
        raise ResetRefused("platform cannot safely open reset parent directory")
    try:
        parent_fd = os.open(
            plan.parent_path,
            os.O_RDONLY | directory | nofollow,
        )
    except OSError as error:
        raise ResetRefused("could not safely open reset parent directory") from error
    parent_stat = os.fstat(parent_fd)
    if (
        parent_stat.st_dev != plan.parent_device
        or parent_stat.st_ino != plan.parent_inode
    ):
        os.close(parent_fd)
        raise ResetRefused("reset parent directory changed after validation")
    return parent_fd


def _verified_target_names(plan: ResetPlan, parent_fd: int) -> tuple[str | None, ...]:
    names: list[str | None] = []
    for target, name in zip(plan.targets, plan.target_names, strict=True):
        try:
            target_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            names.append(None)
            continue
        if stat.S_ISLNK(target_stat.st_mode):
            raise ResetRefused(f"reset target must not be a symlink: {target}")
        if not stat.S_ISREG(target_stat.st_mode):
            raise ResetRefused(f"reset target must be a file: {target}")
        names.append(name)
    return tuple(names)


def _contains_symlink(path: Path) -> bool:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            return True
    return False


def _is_predmarket_command(command: str) -> bool:
    try:
        argv = shlex.split(command)
    except ValueError:
        return False
    if not argv:
        return False
    if Path(argv[0]).name == "predmarket":
        return True
    return any(
        argument == "-m" and argv[index + 1] == "predmarket"
        for index, argument in enumerate(argv[:-1])
    )

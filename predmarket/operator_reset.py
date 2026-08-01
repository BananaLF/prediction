"""Deliberately narrow, operator-only SQLite reset support."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess

try:
    import fcntl
except ImportError:  # pragma: no cover - exercised by the fail-closed lock guard.
    fcntl = None

from predmarket.config import AppConfig


class ResetRefused(ValueError):
    """Raised when a requested database reset is not demonstrably safe."""


TargetIdentity = tuple[int, int]


@dataclass(frozen=True)
class ResetPlan:
    main_path: Path
    targets: tuple[Path, Path, Path]
    parent_path: Path
    target_names: tuple[str, str, str]
    parent_device: int
    parent_inode: int
    target_identities: tuple[TargetIdentity | None, ...]


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
    parent_path = resolved.parent
    try:
        parent_stat = parent_path.stat(follow_symlinks=False)
    except OSError as error:
        raise ResetRefused("could not verify reset parent directory") from error
    if not stat.S_ISDIR(parent_stat.st_mode):
        raise ResetRefused("reset parent directory must be a directory")
    if parent_stat.st_uid != os.geteuid():
        raise ResetRefused("reset parent directory must be owned by this user")
    if parent_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ResetRefused("reset parent directory must not be group or world writable")
    plan = ResetPlan(
        main_path=resolved,
        targets=targets,
        parent_path=parent_path,
        target_names=tuple(target.name for target in targets),
        parent_device=parent_stat.st_dev,
        parent_inode=parent_stat.st_ino,
        target_identities=(None, None, None),
    )
    parent_fd = _open_verified_parent(plan)
    try:
        _lock_parent_directory(parent_fd)
        target_identities = _snapshot_target_identities(plan, parent_fd)
    finally:
        os.close(parent_fd)
    return ResetPlan(
        main_path=plan.main_path,
        targets=plan.targets,
        parent_path=plan.parent_path,
        target_names=plan.target_names,
        parent_device=plan.parent_device,
        parent_inode=plan.parent_inode,
        target_identities=target_identities,
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
    target_descriptors: tuple[tuple[str, int] | None, ...] = ()
    deleted_targets: list[Path] = []
    try:
        _lock_parent_directory(parent_fd)
        target_descriptors = _verified_target_descriptors(plan, parent_fd)
        for target, descriptor in zip(plan.targets, target_descriptors, strict=True):
            if descriptor is not None:
                name, _ = descriptor
                try:
                    os.unlink(name, dir_fd=parent_fd)
                except OSError as error:
                    deleted = ", ".join(str(path) for path in deleted_targets) or "none"
                    raise ResetRefused(
                        "reset partially completed; "
                        f"deleted: {deleted}; failed: {target}"
                    ) from error
                deleted_targets.append(target)
        return tuple(
            target
            for target, descriptor in zip(plan.targets, target_descriptors, strict=True)
            if descriptor is not None
        )
    except OSError as error:
        raise ResetRefused("could not safely remove validated reset target") from error
    finally:
        for descriptor in target_descriptors:
            if descriptor is not None:
                os.close(descriptor[1])
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


def _lock_parent_directory(parent_fd: int) -> None:
    """Serialize cooperating reset operators on the verified directory inode."""
    if fcntl is None:
        raise ResetRefused("platform cannot provide reset advisory locks")
    try:
        fcntl.flock(parent_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        raise ResetRefused("could not acquire exclusive reset directory lock") from error


def _snapshot_target_identities(
    plan: ResetPlan,
    parent_fd: int,
) -> tuple[TargetIdentity | None, ...]:
    identities: list[TargetIdentity | None] = []
    for target, name in zip(plan.targets, plan.target_names, strict=True):
        opened_target = _open_target(target, name, parent_fd)
        if opened_target is None:
            identities.append(None)
            continue
        target_fd, identity = opened_target
        os.close(target_fd)
        identities.append(identity)
    return tuple(identities)


def _verified_target_descriptors(
    plan: ResetPlan,
    parent_fd: int,
) -> tuple[tuple[str, int] | None, ...]:
    descriptors: list[tuple[str, int] | None] = []
    try:
        for target, name, expected_identity in zip(
            plan.targets,
            plan.target_names,
            plan.target_identities,
            strict=True,
        ):
            opened_target = _open_target(target, name, parent_fd)
            if opened_target is None:
                if expected_identity is not None:
                    raise ResetRefused("reset target changed after validation")
                descriptors.append(None)
                continue
            target_fd, actual_identity = opened_target
            if actual_identity != expected_identity:
                os.close(target_fd)
                raise ResetRefused("reset target changed after validation")
            _lock_target_file(target_fd)
            descriptors.append((name, target_fd))
    except BaseException:
        for descriptor in descriptors:
            if descriptor is not None:
                os.close(descriptor[1])
        raise
    return tuple(descriptors)


def _open_target(
    target: Path,
    name: str,
    parent_fd: int,
) -> tuple[int, TargetIdentity] | None:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ResetRefused("platform cannot safely open reset target")
    try:
        target_fd = os.open(
            name,
            os.O_RDONLY | os.O_NONBLOCK | nofollow,
            dir_fd=parent_fd,
        )
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ResetRefused(f"could not safely open reset target: {target}") from error
    try:
        target_stat = os.fstat(target_fd)
    except OSError as error:
        os.close(target_fd)
        raise ResetRefused(f"could not verify reset target: {target}") from error
    if not stat.S_ISREG(target_stat.st_mode):
        os.close(target_fd)
        raise ResetRefused(f"reset target must be a file: {target}")
    return target_fd, (target_stat.st_dev, target_stat.st_ino)


def _lock_target_file(target_fd: int) -> None:
    if fcntl is None:
        os.close(target_fd)
        raise ResetRefused("platform cannot provide reset advisory locks")
    try:
        fcntl.flock(target_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        os.close(target_fd)
        raise ResetRefused("could not acquire exclusive reset target lock") from error


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
    executable = Path(argv[0]).name
    if executable == "predmarket":
        return True
    return _is_python_interpreter(executable) and _has_predmarket_module_entry(argv[1:])


def _is_python_interpreter(executable: str) -> bool:
    return re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", executable) is not None


def _has_predmarket_module_entry(arguments: list[str]) -> bool:
    """Find Python's first module entry while respecting option arguments."""
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument == "-m":
            return index + 1 < len(arguments) and arguments[index + 1] == "predmarket"
        if argument == "-" or not argument.startswith("-"):
            return False
        if argument == "-c" or argument.startswith("-c"):
            return False
        if argument in {"-W", "-X", "--check-hash-based-pycs"}:
            if index + 1 >= len(arguments):
                return False
            index += 2
            continue
        if argument.startswith("-W") or argument.startswith("-X"):
            index += 1
            continue
        if argument in {
            "-b",
            "-B",
            "-d",
            "-E",
            "-h",
            "-i",
            "-I",
            "-O",
            "-OO",
            "-P",
            "-q",
            "-R",
            "-s",
            "-S",
            "-u",
            "-v",
            "-V",
            "-x",
        }:
            index += 1
            continue
        return False
    return False

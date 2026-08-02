from predmarket.cli import main


def run() -> int:
    """Execute the read-only CLI entry point."""
    return main()


if __name__ == "__main__":
    raise SystemExit(run())

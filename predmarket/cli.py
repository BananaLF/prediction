"""Read-only command-line interface."""

from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only Polymarket signal service")
    parser.parse_args()

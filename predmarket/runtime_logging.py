"""Runtime logging configuration for the command-line service."""

from __future__ import annotations

import logging
from typing import TextIO


_LOGGER_NAME = "predmarket"
_HANDLER_MARKER = "_predmarket_runtime_handler"


def configure_runtime_logging(output: TextIO) -> None:
    """Send predmarket runtime logs to the CLI output stream at INFO level."""

    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in tuple(logger.handlers):
        if not getattr(handler, _HANDLER_MARKER, False):
            continue
        logger.removeHandler(handler)
        handler.close()

    handler = logging.StreamHandler(output)
    setattr(handler, _HANDLER_MARKER, True)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    logger.addHandler(handler)

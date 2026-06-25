"""Entry point for the AML transaction generator."""

from __future__ import annotations

import logging
import sys

from src.generator.transaction_generator import create_producer, run_loop

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","module":"generator","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


def run() -> None:
    try:
        run_loop()
    except Exception as exc:
        logger.exception("Generator failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    run()

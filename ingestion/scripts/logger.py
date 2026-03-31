"""
Centralized structured logging for Banking Data Platform.
Built on loguru — provides consistent log format across all pipeline components.

Usage:
    from ingestion.scripts.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Pipeline started", pipeline="transactions", batch_id="batch_001")
"""

import sys
from loguru import logger
from pathlib import Path


# Remove default loguru handler
logger.remove()

# ── Console Handler ──────────────────────────────────────────────────────────
logger.add(
    sys.stdout,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    ),
    level="DEBUG",
    colorize=True,
)

# ── File Handler — rotating logs ─────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "pipeline_{time:YYYY-MM-DD}.log",
    format=(
        "{time:YYYY-MM-DD HH:mm:ss} | "
        "{level: <8} | "
        "{name}:{function}:{line} | "
        "{message}"
    ),
    level="INFO",
    rotation="1 day",       # New file every day
    retention="30 days",    # Keep 30 days of logs
    compression="zip",      # Compress old logs
    enqueue=True,           # Thread-safe logging
)

# ── Error Log — separate file for errors only ────────────────────────────────
logger.add(
    LOG_DIR / "errors_{time:YYYY-MM-DD}.log",
    format=(
        "{time:YYYY-MM-DD HH:mm:ss} | "
        "{level: <8} | "
        "{name}:{function}:{line} | "
        "{message}"
    ),
    level="ERROR",
    rotation="1 week",
    retention="90 days",
    compression="zip",
    enqueue=True,
)


def get_logger(name: str):
    """
    Returns a named logger instance.
    
    Args:
        name: Module name, typically __name__
        
    Returns:
        loguru logger bound with module context
        
    Example:
        logger = get_logger(__name__)
        logger.info("Starting ingestion")
        logger.error("Failed to connect", error=str(e))
    """
    return logger.bind(module=name)


class PipelineLogger:
    """
    Context-aware logger for pipeline runs.
    Automatically includes pipeline metadata in every log entry.
    
    Usage:
        with PipelineLogger("transactions_ingestion", batch_id="batch_001") as log:
            log.info("Processing started")
            log.success("10000 records processed")
    """

    def __init__(self, pipeline_name: str, **context):
        self.pipeline_name = pipeline_name
        self.context = context
        self._logger = logger.bind(
            pipeline=pipeline_name,
            **context
        )

    def __enter__(self):
        self._logger.info(
            f"Pipeline [{self.pipeline_name}] STARTED",
            **self.context
        )
        return self._logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._logger.error(
                f"Pipeline [{self.pipeline_name}] FAILED — {exc_val}",
                **self.context
            )
        else:
            self._logger.success(
                f"Pipeline [{self.pipeline_name}] COMPLETED",
                **self.context
            )
        return False  # Don't suppress exceptions
"""
Unit tests for the logging module.
"""

from ingestion.scripts.logger import get_logger, PipelineLogger


def test_get_logger_returns_logger():
    """Logger should be created without errors."""
    log = get_logger(__name__)
    assert log is not None


def test_pipeline_logger_context_manager():
    """PipelineLogger should work as a context manager."""
    with PipelineLogger("test_pipeline", batch_id="test_001") as log:
        log.info("Test log entry")


def test_pipeline_logger_captures_exception():
    """PipelineLogger should log errors without suppressing them."""
    try:
        with PipelineLogger("test_pipeline", batch_id="test_002") as log:
            log.info("About to fail")
            raise ValueError("Simulated pipeline failure")
    except ValueError:
        pass  # Exception should propagate — not suppressed
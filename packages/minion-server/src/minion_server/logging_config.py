"""JSON logging for the server and watchdog (structlog), shared so both emit the same shape.

Every line is one JSON object with `timestamp` (ISO 8601 UTC), `level` and `event` plus the
bound fields. Callers log ids and counts only, never payloads, Authorization headers, presigned
URLs or tokens (ARCHITECTURE §Logging).
"""

import logging
import sys

import structlog

_SHARED: list[structlog.typing.Processor] = [
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
]


def configure_logging(level: int = logging.INFO) -> None:
    """Route structlog and stdlib logging (uvicorn, sqlalchemy) to JSON on stdout. Idempotent."""
    structlog.configure(
        processors=[*_SHARED, structlog.processors.JSONRenderer()],
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        wrapper_class=structlog.make_filtering_bound_logger(level),
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=[structlog.stdlib.ExtraAdder(), *_SHARED],
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]  # replace, so a second call never duplicates lines
    root.setLevel(level)

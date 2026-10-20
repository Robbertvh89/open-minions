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
    # Plain text traceback only: never dict_tracebacks/show_locals (locals may hold secrets).
    structlog.processors.format_exc_info,
]


class _StripQueryFilter(logging.Filter):
    """uvicorn.access args are (client, method, path?query, http_version, status): drop the query.

    Query strings can carry presigned-URL signatures or tokens; they never reach a log line.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and len(args) >= 3 and isinstance(args[2], str):
            record.args = (*args[:2], args[2].split("?", 1)[0], *args[3:])
        return True


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
            foreign_pre_chain=list(_SHARED),
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]  # replace, so a second call never duplicates lines
    root.setLevel(level)
    for noisy in ("httpx", "httpcore"):  # httpx logs full URLs (incl. X-Amz-Signature) at INFO
        logging.getLogger(noisy).setLevel(logging.WARNING)
    # uvicorn's default LOGGING_CONFIG (any earlier uvicorn.Config, or the `uvicorn` CLI) leaves
    # these with their own plain handlers and propagate=False: hand them back to the root handler.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers[:] = []
        lg.propagate = True
        lg.setLevel(logging.NOTSET)
    access = logging.getLogger("uvicorn.access")
    if not any(isinstance(f, _StripQueryFilter) for f in access.filters):
        access.addFilter(_StripQueryFilter())

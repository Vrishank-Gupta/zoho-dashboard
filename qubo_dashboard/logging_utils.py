from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import Request

from .config import settings


RUNTIME_LOGGER_NAME = "qubo.runtime"
ACCESS_LOGGER_NAME = "qubo.access"
AUDIT_LOGGER_NAME = "qubo.audit"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=True)


def setup_logging() -> None:
    log_dir = Path(settings.app_log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    if getattr(root, "_qubo_logging_configured", False):
        return

    root.setLevel(getattr(logging, settings.app_log_level, logging.INFO))
    root.handlers.clear()

    formatter = JsonFormatter()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    for logger_name, filename in (
        (RUNTIME_LOGGER_NAME, settings.app_runtime_log_name),
        (ACCESS_LOGGER_NAME, settings.app_access_log_name),
        (AUDIT_LOGGER_NAME, settings.app_audit_log_name),
    ):
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, settings.app_log_level, logging.INFO))
        logger.propagate = False
        logger.handlers.clear()

        file_handler = RotatingFileHandler(
            log_dir / filename,
            maxBytes=settings.app_log_max_bytes,
            backupCount=settings.app_log_backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    uvicorn_access = logging.getLogger("uvicorn.access")
    uvicorn_access.handlers.clear()
    uvicorn_access.propagate = False
    uvicorn_access.setLevel(logging.WARNING)

    uvicorn_error = logging.getLogger("uvicorn.error")
    uvicorn_error.handlers.clear()
    uvicorn_error.propagate = True

    root._qubo_logging_configured = True  # type: ignore[attr-defined]


def runtime_logger() -> logging.Logger:
    return logging.getLogger(RUNTIME_LOGGER_NAME)


def access_logger() -> logging.Logger:
    return logging.getLogger(ACCESS_LOGGER_NAME)


def audit_logger() -> logging.Logger:
    return logging.getLogger(AUDIT_LOGGER_NAME)


def make_request_id() -> str:
    return uuid4().hex


def client_ip(request: Request) -> str:
    if settings.app_trust_forwarded_headers:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            first = forwarded.split(",")[0].strip()
            if first:
                return first
        real_ip = request.headers.get("x-real-ip", "").strip()
        if real_ip:
            return real_ip
    return request.client.host if request.client else "unknown"


def header_subset(request: Request) -> dict[str, str]:
    headers: dict[str, str] = {}
    for key in ("user-agent", "referer", "origin", "x-forwarded-for", "x-real-ip"):
        value = request.headers.get(key)
        if value:
            headers[key] = value[:500]
    return headers


def log_access(
    request: Request,
    *,
    status_code: int,
    duration_ms: float,
    request_id: str,
) -> None:
    access_logger().info(
        "request_complete",
        extra={
            "extra_fields": {
                "event": "request_complete",
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "query": str(request.url.query or "")[:1000],
                "status_code": status_code,
                "duration_ms": round(duration_ms, 2),
                "client_ip": client_ip(request),
                "headers": header_subset(request),
            }
        },
    )


def log_audit(
    request: Request,
    *,
    action: str,
    outcome: str = "success",
    details: dict[str, Any] | None = None,
) -> None:
    audit_logger().info(
        action,
        extra={
            "extra_fields": {
                "event": "audit",
                "action": action,
                "outcome": outcome,
                "request_id": getattr(request.state, "request_id", ""),
                "method": request.method,
                "path": request.url.path,
                "client_ip": client_ip(request),
                "headers": header_subset(request),
                "details": details or {},
            }
        },
    )

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock, Thread
from typing import Any

from ..clickhouse_analytics import ClickHouseETLJob
from ..config import settings
from .run import run_pipeline


@dataclass
class PipelineState:
    running: bool = False
    last_started_at: str | None = None
    last_finished_at: str | None = None
    last_status: str | None = None
    last_message: str | None = None
    last_duration_seconds: float | None = None
    requested_by: str | None = None
    history: list[dict[str, Any]] = field(default_factory=list)


class PipelineManager:
    def __init__(self) -> None:
        self._lock = Lock()
        self._state = PipelineState()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "running": self._state.running,
                "last_started_at": self._state.last_started_at,
                "last_finished_at": self._state.last_finished_at,
                "last_status": self._state.last_status,
                "last_message": self._state.last_message,
                "last_duration_seconds": self._state.last_duration_seconds,
                "requested_by": self._state.requested_by,
                "history": list(self._state.history[-5:]),
            }

    def start(self, requested_by: str = "dashboard") -> dict[str, Any]:
        with self._lock:
            if self._state.running:
                return {"accepted": False, "reason": "Pipeline is already running.", "status": self.status()}
            self._state.running = True
            self._state.last_started_at = datetime.utcnow().isoformat()
            self._state.last_finished_at = None
            self._state.last_status = "Running"
            self._state.last_message = "Pipeline started"
            self._state.requested_by = requested_by
            self._state.last_duration_seconds = None

        thread = Thread(target=self._run, daemon=True)
        thread.start()
        return {"accepted": True, "reason": "Pipeline started.", "status": self.status()}

    def _run(self) -> None:
        started = datetime.utcnow()
        status = "Success"
        message = "Pipeline completed"
        try:
            if settings.analytics_backend == "clickhouse" and settings.has_clickhouse:
                result = ClickHouseETLJob().run()
                message = result.message
            else:
                run_pipeline()
        except Exception as exc:
            status = "Failed"
            message = str(exc)
        finished = datetime.utcnow()
        duration = (finished - started).total_seconds()
        with self._lock:
            self._state.running = False
            self._state.last_finished_at = finished.isoformat()
            self._state.last_status = status
            self._state.last_message = message
            self._state.last_duration_seconds = duration
            self._state.history.append(
                {
                    "started_at": started.isoformat(),
                    "finished_at": finished.isoformat(),
                    "status": status,
                    "message": message,
                    "duration_seconds": duration,
                    "requested_by": self._state.requested_by,
                }
            )

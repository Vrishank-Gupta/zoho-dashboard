from __future__ import annotations

import uvicorn

from .config import settings


def main() -> None:
    uvicorn.run(
        "qubo_dashboard.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.app_reload,
    )


if __name__ == "__main__":
    main()

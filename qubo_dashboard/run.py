from __future__ import annotations

import uvicorn

from .config import settings
from .logging_utils import setup_logging


def main() -> None:
    setup_logging()
    uvicorn.run(
        "qubo_dashboard.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.app_reload,
        access_log=False,
        proxy_headers=settings.app_trust_forwarded_headers,
        forwarded_allow_ips="*",
    )


if __name__ == "__main__":
    main()

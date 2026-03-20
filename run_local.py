#!/usr/bin/env python3
"""
Local development runner for Qubo Support Health Dashboard.

Starts the FastAPI backend and serves the frontend, then opens the browser.
Press Ctrl+C to stop everything.
"""
from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

from qubo_dashboard.config import settings

# ── Configuration ────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent.resolve()

BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8010
FRONTEND_PORT = 5500

BACKEND_URL = f"http://{BACKEND_HOST}:{BACKEND_PORT}"
FRONTEND_URL = f"http://127.0.0.1:{FRONTEND_PORT}"

# ── Helpers ──────────────────────────────────────────────────────────────────

def info(msg: str) -> None:
    print(f"  [INFO]  {msg}")

def ok(msg: str) -> None:
    print(f"  [ OK ]  {msg}")

def warn(msg: str) -> None:
    print(f"  [WARN]  {msg}")

def error(msg: str) -> None:
    print(f"  [ERR ]  {msg}", file=sys.stderr)

def header(title: str) -> None:
    width = 60
    print()
    print("-" * width)


def is_port_in_use(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        return sock.connect_ex((host, port)) == 0
    print(f"  {title}")
    print("-" * width)

# ── Preflight checks ─────────────────────────────────────────────────────────

def check_python_version() -> None:
    if sys.version_info < (3, 10):
        error(f"Python 3.10+ required, found {sys.version}")
        sys.exit(1)
    ok(f"Python {sys.version.split()[0]}")


def check_env_file() -> None:
    env_path = ROOT / ".env"
    example_path = ROOT / ".env.example"
    if not env_path.exists():
        if example_path.exists():
            shutil.copy(example_path, env_path)
            warn(".env not found - copied from .env.example. Fill in your credentials before running again.")
            sys.exit(1)
        else:
            error(".env file not found. Create one based on .env.example.")
            sys.exit(1)
    ok(".env found")


def check_ports() -> None:
    conflicts: list[str] = []
    if is_port_in_use(BACKEND_HOST, BACKEND_PORT):
        conflicts.append(f"Backend port {BACKEND_PORT} is already in use on {BACKEND_HOST}")
    if is_port_in_use("127.0.0.1", FRONTEND_PORT):
        conflicts.append(f"Frontend port {FRONTEND_PORT} is already in use on 127.0.0.1")
    if conflicts:
        for msg in conflicts:
            error(msg)
        error("Stop the existing process or change the ports before running run_local.py")
        sys.exit(1)
    ok(f"Ports {BACKEND_PORT} and {FRONTEND_PORT} are free")


def install_requirements() -> None:
    req_path = ROOT / "requirements.txt"
    if not req_path.exists():
        warn("requirements.txt not found, skipping pip install")
        return

    info("Installing/verifying Python dependencies ...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req_path), "--quiet"],
        cwd=ROOT,
    )
    if result.returncode != 0:
        error("pip install failed. Check requirements.txt and your Python environment.")
        sys.exit(1)
    ok("Dependencies ready")


def patch_frontend_config() -> None:
    """Ensure frontend/config.js points to the local backend."""
    config_path = ROOT / "frontend" / "config.js"
    correct_content = f'window.QUBO_APP_CONFIG = {{\n  apiBaseUrl: "{BACKEND_URL}",\n}};\n'

    if config_path.exists():
        if config_path.read_text(encoding="utf-8").strip() == correct_content.strip():
            ok("frontend/config.js already points to local backend")
            return
        # Back up original content the first time
        backup = config_path.with_suffix(".js.bak")
        if not backup.exists():
            shutil.copy(config_path, backup)
            info("Backed up original config.js -> config.js.bak")

    config_path.write_text(correct_content, encoding="utf-8")
    ok(f"frontend/config.js -> {BACKEND_URL}")

# ── Process launchers ─────────────────────────────────────────────────────────

def start_backend() -> subprocess.Popen:
    cmd = [
        sys.executable, "-m", "uvicorn",
        "qubo_dashboard.main:app",
        "--host", BACKEND_HOST,
        "--port", str(BACKEND_PORT),
    ]
    if settings.app_reload:
        cmd.append("--reload")
    info(f"Starting backend on {BACKEND_URL} ...")
    proc = subprocess.Popen(cmd, cwd=ROOT)
    return proc


def start_frontend() -> subprocess.Popen:
    frontend_dir = ROOT / "frontend"
    cmd = [
        sys.executable, "-m", "http.server", str(FRONTEND_PORT),
        "--directory", str(frontend_dir),
        "--bind", "127.0.0.1",
    ]
    info(f"Serving frontend on {FRONTEND_URL} ...")
    proc = subprocess.Popen(cmd, cwd=ROOT)
    return proc


def wait_for_backend(timeout: int = 30) -> bool:
    """Poll until the backend /api/health responds or timeout expires."""
    import urllib.error
    import urllib.request

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{BACKEND_URL}/api/health", timeout=2):
                return True
        except Exception:
            time.sleep(1)
    return False

# ── Optional pipeline run ────────────────────────────────────────────────────

def check_agg_db() -> bool:
    """Return True if the local analytics DB is reachable."""
    try:
        import mysql.connector
        from qubo_dashboard.config import settings
        cfg = settings.agg_db
        if not cfg.is_configured:
            return False
        conn = mysql.connector.connect(
            host=cfg.host, port=cfg.port,
            user=cfg.user, password=cfg.password,
            database=cfg.database,
            connection_timeout=5,
        )
        conn.close()
        return True
    except Exception:
        return False


def maybe_run_pipeline() -> None:
    answer = input("\n  Run the ETL pipeline now to load fresh data from Zoho? [y/N] ").strip().lower()
    if answer != "y":
        info("Skipping pipeline run")
        return

    # Verify agg DB is reachable before attempting pipeline
    info("Checking local analytics DB connection ...")
    if not check_agg_db():
        warn("Cannot reach local analytics DB. Run setup_local_db.py first:")
        warn("    python setup_local_db.py")
        warn("Skipping pipeline - dashboard will use sample data as fallback.")
        return

    ok("Local analytics DB is reachable")
    info("Fetching tickets from remote Zoho MySQL and writing aggregates ...")
    result = subprocess.run(
        [sys.executable, "-m", "qubo_dashboard.pipeline.run"],
        cwd=ROOT,
    )
    if result.returncode == 0:
        ok("Pipeline completed - local DB is populated with real data")
    else:
        warn("Pipeline exited with errors (dashboard will still start with sample data)")

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    header("Qubo Support Health Dashboard - Local Runner")

    # ── Preflight ──────────────────────────────────────────────────────────
    header("Preflight checks")
    check_python_version()
    check_env_file()
    check_ports()
    install_requirements()
    patch_frontend_config()

    # ── Optional pipeline ─────────────────────────────────────────────────
    maybe_run_pipeline()

    # ── Start services ────────────────────────────────────────────────────
    header("Starting services")
    processes: list[subprocess.Popen] = []

    backend = start_backend()
    processes.append(backend)

    frontend = start_frontend()
    processes.append(frontend)

    # ── Wait for backend to be ready ──────────────────────────────────────
    info("Waiting for backend to be ready ...")
    if wait_for_backend():
        ok("Backend is up")
    else:
        error("Backend did not respond within 30 s. Not opening the dashboard because the frontend would stay in loading state.")
        error(f"Check the backend process output, then verify {BACKEND_URL}/api/health")
        for proc in processes:
            proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        sys.exit(1)

    # ── Open browser ──────────────────────────────────────────────────────
    header("Dashboard ready")
    print(f"  Frontend : {FRONTEND_URL}")
    print(f"  API docs : {BACKEND_URL}/docs")
    print(f"  Health   : {BACKEND_URL}/api/health")
    print()
    print("  Press Ctrl+C to stop all services.")
    print()

    webbrowser.open(FRONTEND_URL)

    # ── Keep alive until Ctrl+C ───────────────────────────────────────────
    def shutdown(sig, frame):
        header("Shutting down")
        for proc in processes:
            proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        ok("All services stopped. Goodbye!")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Wait for any child to exit unexpectedly
    while True:
        for proc in processes:
            if proc.poll() is not None:
                error(f"A service (PID {proc.pid}) exited unexpectedly (code {proc.returncode})")
                shutdown(None, None)
        time.sleep(2)


if __name__ == "__main__":
    main()

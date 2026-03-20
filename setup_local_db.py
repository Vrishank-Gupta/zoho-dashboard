#!/usr/bin/env python3
"""
One-time local MySQL setup for Qubo Analytics.

Creates the 'analytics' database and 'llm_reader' user on your local MySQL,
then runs the ETL pipeline to populate the aggregate tables from Zoho.

Usage:
    python setup_local_db.py
"""
from __future__ import annotations

import getpass
import sys


def info(msg: str) -> None:
    print(f"  [INFO]  {msg}")

def ok(msg: str) -> None:
    print(f"  [ OK ]  {msg}")

def error(msg: str) -> None:
    print(f"  [ERR ]  {msg}", file=sys.stderr)

def header(title: str) -> None:
    width = 60
    print()
    print("─" * width)
    print(f"  {title}")
    print("─" * width)


def get_mysql_connector():
    try:
        import mysql.connector
        return mysql.connector
    except ImportError:
        error("mysql-connector-python is not installed.")
        error("Run: pip install mysql-connector-python")
        sys.exit(1)


def connect_as_admin(connector, host: str, port: int, user: str, password: str):
    try:
        conn = connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
        )
        return conn
    except Exception as e:
        error(f"Could not connect to MySQL as '{user}': {e}")
        return None


def setup_database(host: str = "127.0.0.1", port: int = 3306) -> bool:
    header("Local MySQL Setup")

    connector = get_mysql_connector()

    # Load target credentials from .env
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    import os
    agg_host   = os.getenv("QUBO_AGG_DB_HOST", "127.0.0.1")
    agg_port   = int(os.getenv("QUBO_AGG_DB_PORT", "3306"))
    agg_user   = os.getenv("QUBO_AGG_DB_USER", "llm_reader")
    agg_pass   = os.getenv("QUBO_AGG_DB_PASSWORD", "")
    agg_db     = os.getenv("QUBO_AGG_DB_NAME", "analytics")

    info(f"Target:  {agg_user}@{agg_host}:{agg_port}/{agg_db}")

    # First try connecting directly as the agg user — maybe it already works
    info(f"Checking if '{agg_user}' can already connect …")
    try:
        test_conn = connector.connect(
            host=agg_host, port=agg_port,
            user=agg_user, password=agg_pass,
            database=agg_db,
        )
        test_conn.close()
        ok(f"'{agg_user}'@'{agg_db}' is already accessible — no setup needed.")
        return True
    except Exception:
        pass

    # Need admin access to create the DB/user
    print()
    print("  The analytics database or user doesn't exist yet.")
    print("  Enter your MySQL admin credentials to set them up.")
    print()
    admin_user = input(f"  MySQL admin username [root]: ").strip() or "root"
    admin_pass = getpass.getpass(f"  MySQL admin password for '{admin_user}': ")

    conn = connect_as_admin(connector, agg_host, agg_port, admin_user, admin_pass)
    if conn is None:
        return False

    cursor = conn.cursor()

    # Create database
    info(f"Creating database '{agg_db}' if not exists …")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{agg_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    ok(f"Database '{agg_db}' ready")

    # Create user (handle both localhost and 127.0.0.1 bindings)
    for host_binding in ["localhost", "127.0.0.1", "%"]:
        try:
            cursor.execute(
                f"CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s",
                (agg_user, host_binding, agg_pass),
            )
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON `{agg_db}`.* TO %s@%s",
                (agg_user, host_binding),
            )
        except Exception as e:
            info(f"  Skipped binding '{host_binding}': {e}")

    cursor.execute("FLUSH PRIVILEGES")
    ok(f"User '{agg_user}' created and granted access to '{agg_db}'")

    conn.commit()
    cursor.close()
    conn.close()

    # Verify the new user can connect
    info("Verifying new user can connect …")
    try:
        verify_conn = connector.connect(
            host=agg_host, port=agg_port,
            user=agg_user, password=agg_pass,
            database=agg_db,
        )
        verify_conn.close()
        ok("Connection verified successfully")
        return True
    except Exception as e:
        error(f"Verification failed: {e}")
        return False


def run_pipeline() -> bool:
    header("Running ETL Pipeline")
    info("Fetching tickets from remote Zoho MySQL …")
    info("(This may take a minute depending on data volume)")
    print()

    try:
        from qubo_dashboard.pipeline.run import run_pipeline as _run
        _run()
        return True
    except Exception as e:
        error(f"Pipeline failed: {e}")
        return False


def main() -> None:
    print()
    print("  Qubo Analytics — Local Database Setup")
    print()

    if not setup_database():
        error("Database setup failed. Fix the errors above and try again.")
        sys.exit(1)

    print()
    answer = input("  Run the ETL pipeline now to load data from Zoho? [Y/n] ").strip().lower()
    if answer in ("", "y", "yes"):
        success = run_pipeline()
        if success:
            header("Done")
            print("  Local analytics DB is populated.")
            print("  Start the dashboard with:  python run_local.py")
        else:
            sys.exit(1)
    else:
        header("Done")
        print("  Database is ready. Run the pipeline whenever you want:")
        print("  python -m qubo_dashboard.pipeline.run")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
from datetime import date

from .etl import ClickHouseETLJob


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()
    job = ClickHouseETLJob()
    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise SystemExit("Both --start-date and --end-date are required for manual backfill.")
        result = job.run_for_date_range(date.fromisoformat(args.start_date), date.fromisoformat(args.end_date))
    else:
        result = job.run()
    print(result.message)


if __name__ == "__main__":
    main()

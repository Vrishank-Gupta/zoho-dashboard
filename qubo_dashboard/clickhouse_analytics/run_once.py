from __future__ import annotations

from .etl import ClickHouseETLJob


def main() -> None:
    result = ClickHouseETLJob().run()
    print(result.message)


if __name__ == "__main__":
    main()

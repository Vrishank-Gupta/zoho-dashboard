from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from ..config import settings
from .etl import ClickHouseETLJob


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def run_job() -> None:
    LOGGER.info("Starting ETL job %s", settings.etl_job_name)
    result = ClickHouseETLJob().run()
    LOGGER.info(result.message)


def main() -> None:
    scheduler = BlockingScheduler(timezone=settings.etl_timezone)
    scheduler.add_job(
        run_job,
        CronTrigger.from_crontab(settings.etl_schedule, timezone=settings.etl_timezone),
        id=settings.etl_job_name,
        name=settings.etl_job_name,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    LOGGER.info(
        "Scheduler active for %s with cron '%s' in timezone '%s'",
        settings.etl_job_name,
        settings.etl_schedule,
        settings.etl_timezone,
    )
    scheduler.start()


if __name__ == "__main__":
    main()

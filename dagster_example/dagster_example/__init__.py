from dagster import Definitions, load_assets_from_modules

# from . import assets
from .jobs import (
    test_job,
    exchange_rates_job,
    weather_msk_job
    )

from .schedules import (
    test_job_sch,
    exchange_rates_job_sch,
    weather_msk_job_sch
    )


# all_assets = load_assets_from_modules([assets])

defs = Definitions(
    # assets=all_assets,
    jobs=[test_job, exchange_rates_job, weather_msk_job],
    schedules=[test_job_sch, exchange_rates_job_sch, weather_msk_job_sch]
)

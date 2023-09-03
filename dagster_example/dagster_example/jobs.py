from dagster import job

from .ops.test_op import test
from .ops.exchange_rates import (
    result_from_api,
    load_result
    )
from .ops.weather_msk import (
    weather_msk,
    weather_msk_to_df
    )


@job
def test_job():
    test()


@job
def exchange_rates_job():
    load_result(df=result_from_api())


@job
def weather_msk_job():
    weather_msk_to_df(weather_info=weather_msk())
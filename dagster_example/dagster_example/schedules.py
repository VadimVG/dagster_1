from dagster import schedule

from .jobs import (
    test_job,
    exchange_rates_job,
    weather_msk_job
    )

##############################
@schedule(
    cron_schedule="0 9 * * 1-5",
    job=test_job,
    execution_timezone="Europe/Moscow"
)
def test_job_sch(context):
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {"ops": {"download_cereals": {"config": {"date": date}}}}
##############################

##############################
@schedule(
    cron_schedule="5 12 * * *",
    job=exchange_rates_job,
    execution_timezone="Europe/Moscow"
)
def exchange_rates_job_sch(context):
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {"ops": {"download_cereals": {"config": {"date": date}}}}
##############################

@schedule(
    cron_schedule="0 10 * * *",
    job=weather_msk_job,
    execution_timezone="Europe/Moscow"
)
def weather_msk_job_sch(context):
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {"ops": {"download_cereals": {"config": {"date": date}}}}
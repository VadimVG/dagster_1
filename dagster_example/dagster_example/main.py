from .jobs import (
    test_job,
    exchange_rates_job,
    weather_msk_job
    )

if __name__=='__main__':
    process=test_job.execute_in_process()
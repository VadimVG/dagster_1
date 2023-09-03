from dagster import op, RetryRequested, Nothing

import requests
import pandas as pd
import datetime


#получение курса валют по api ЦБ РФ
@op
def result_from_api(context) -> pd.DataFrame:
    try:
        date=datetime.date.today()
        year, month, day=str(date).split('-')
        url=f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={day}/{month}/{year}'
        response=requests.get(url=url)
        df=pd.read_xml(response.text)
        context.log.info(df)
        return df
    except Exception as e:
        raise RetryRequested(
            max_retries=2,
            seconds_to_wait=10
            ) from e



# инкрементная загрузка данных в хранилище
@op
def load_result(context, df: pd.DataFrame) -> Nothing:
    conn='connection to db'
    df=df
    df['tech_load_date']=datetime.datetime.today()
    # df.to_sql(name='table_name', schema='schema_name', if_exists='append', index=False, con=conn)
    context.log.info('done')
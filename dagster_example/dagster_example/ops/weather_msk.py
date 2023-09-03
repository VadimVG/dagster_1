from dagster import op, Nothing

import pandas as pd
import requests
import datetime
from bs4 import BeautifulSoup



headers = {'accept': '*/*',
   'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}


#получение погоды в мск на 14 дней
@op
def weather_msk() -> dict:
    error_logs={}
    total_d={
        'date':[],
        'temp':[],
        'feels_like':[],
        'probability':[],
        'pressure':[],
        'wind':[],
        'air_humidity':[]
    }

    url='https://world-weather.ru/pogoda/russia/moscow/14days/'
    response=requests.get(url=url, headers=headers)
    if response.status_code==200:
        soup=BeautifulSoup(response.text, 'lxml')
        data=soup.find_all(class_='weather-short')
        for val in data:
            try:
                date=val.get('id')

                pre_temp=val.find(class_='day fourteen-d'
                                    ).find(class_='weather-temperature'
                                           ).find('span').text
                temp=int(pre_temp.replace('°', ''))

                pre_feels_like=val.find(class_='day fourteen-d'
                               ).find(class_='weather-temperature').text
                feels_like=int(pre_feels_like.replace('°', '')) if len(pre_feels_like) > 1 else 0

                pre_probability=val.find(class_='day fourteen-d'
                               ).find(class_='weather-probability').text
                probability=int(pre_probability.replace('%', ''))

                pre_pressure=val.find(class_='day fourteen-d'
                               ).find(class_='weather-pressure').text
                pressure=int(pre_pressure) if len(pre_pressure) > 1 else 0

                pre_wind=val.find(class_='day fourteen-d'
                               ).find(class_='weather-wind').text
                wind=float(pre_wind)

                pre_air_humidity=val.find(class_='day fourteen-d'
                               ).find(class_='weather-humidity').text
                air_humidity=int(pre_air_humidity.replace('%', '')) if len(pre_air_humidity)>1 else 0

                total_d['date'].append(date)
                total_d['temp'].append(temp)
                total_d['feels_like'].append(feels_like)
                total_d['probability'].append(probability)
                total_d['pressure'].append(pressure)
                total_d['wind'].append(wind)
                total_d['air_humidity'].append(air_humidity)
            except Exception as e:
                error_logs[date]=str(e)
    else:
        raise Exception(f'Error. Status code - {response.status_code}')
    if len(error_logs)>0:
        raise Exception(error_logs.items())
    else:
        return total_d


# добавление новых атрибутов, загрузка в бд
@op
def weather_msk_to_df(context, weather_info:dict) -> Nothing:
    conn = 'connection to db'
    weather_info=weather_info
    df=pd.DataFrame(weather_info)
    df['action_type']=''
    for row in range(len(df)):
        if df['probability'][row] in range(15, 30):
            df['action_type'][row]=1
        elif df['probability'][row] in range(30, 60):
            df['action_type'][row] = 2
        elif df['probability'][row] in range(60, 100+1):
            df['action_type'][row] = 3
        else:
            df['action_type'][row]=0
    df['tech_load_date'] = datetime.datetime.today()
    context.log.info(df)
    # df.to_sql(name='table_name', schema='schema_name', if_exists='replace', index=False, con=conn)
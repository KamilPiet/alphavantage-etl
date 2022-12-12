import os
import requests
import pandas as pd
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from datetime import datetime


# extract tasks
# extract from alphavantage and load to local postgresql database daily price (in USD) of a given symbol
@task()
def get_daily_price():
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}' \
          f'&apikey={os.getenv("ALPHAVANTAGE_API_KEY")}'
    r = requests.get(url)
    data = r.json()
    df_price_usd = pd.DataFrame(data["Time Series (Daily)"]).transpose()
    for col in df_price_usd:
        df_price_usd[col] = pd.to_numeric(df_price_usd[col])
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df_price_usd.to_sql(f'src_{symbol.lower()}_price_usd', engine, if_exists='replace', index=True)


# extract from alphavantage and load to local postgresql database daily exchange rate between USD and given currency
@task()
def get_daily_exchange_rate():
    url = f'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol={currency}' \
          f'&apikey={os.getenv("ALPHAVANTAGE_API_KEY")}'
    r = requests.get(url)
    data = r.json()
    df_exchange_rate = pd.DataFrame(data["Time Series FX (Daily)"]).transpose()
    for col in df_exchange_rate:
        df_exchange_rate[col] = pd.to_numeric(df_exchange_rate[col])
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df_exchange_rate.to_sql(f'src_usd_{currency.lower()}', engine, if_exists='replace', index=True)


with DAG(dag_id="alphavantage_etl_dag", schedule_interval="*/5 * * * *", start_date=datetime(2022, 12, 1),
         catchup=False, tags=["alphavantage"]) as dag:

    symbol = "SPY"
    currency = "PLN"

    with TaskGroup("extract_load_src", tooltip="Extract and load symbol price in USD and currency exchange rate") \
            as extract_load_src:
        src_daily_price = get_daily_price()
        src_daily_exchange_rate = get_daily_exchange_rate()
        # order
        [src_daily_price, src_daily_exchange_rate]

    # order
    extract_load_src

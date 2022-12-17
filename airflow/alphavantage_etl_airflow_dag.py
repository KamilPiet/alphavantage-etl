import os
import requests
import pandas as pd
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine, exc
from datetime import datetime, date
import time
import numpy as np


# extract tasks
# extract from alphavantage and load to local postgresql database daily price (in USD) of a given symbol
@task()
def get_daily_price():
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df_holidays = pd.read_sql_query(f'SELECT * FROM public.holidays', engine)
    holidays = df_holidays.to_numpy(dtype='datetime64').flatten()
    try:
        df_recent = pd.read_sql_query(f'SELECT date FROM public.src_{symbol.lower()}_price_usd ORDER BY date DESC '
                                      f'LIMIT 1', engine)
        recent = datetime.strptime(df_recent.iloc[0].iat[0], '%Y-%m-%d').date()
        date_diff = np.busday_count(recent, date.today(), holidays=holidays) - 1
        print(f'{date_diff} business days behind')
        if date_diff <= 1:
            print('No need to pull data')
            return
        elif date_diff <= 100:
            output_size = 'compact'
            print('Pulling 100 rows')
        else:
            output_size = 'full'
            print('Pulling all available data')
        table_exists = True
    except exc.ProgrammingError as e:  # to catch an exception, when the table does not exist
        print(e)
        output_size = 'full'
        table_exists = False
        print('Pulling all available data')
    except:                            # to catch other exceptions
        output_size = 'full'
        table_exists = False
        print('Pulling all available data')

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}' \
          f'&outputsize={output_size}&apikey={os.getenv("ALPHAVANTAGE_API_KEY")}'
    r = requests.get(url)
    data = r.json()
    # convert time series to df then transpose df and reverse rows
    df_price_usd = pd.DataFrame(data["Time Series (Daily)"]).transpose().iloc[::-1]

    if table_exists:
        df_price_usd = df_price_usd.tail(date_diff)  # keep only those rows, that are missing from the database
    for col in df_price_usd:
        df_price_usd[col] = pd.to_numeric(df_price_usd[col])

    df_price_usd.to_sql(f'src_{symbol.lower()}_price_usd', engine, if_exists='append', index=True, index_label='date')
    if not table_exists:
        with engine.connect() as con:
            con.execute(f'ALTER TABLE src_{symbol.lower()}_price_usd ADD PRIMARY KEY (date);')


# extract from alphavantage and load to local postgresql database daily exchange rate between USD and given currency
@task()
def get_daily_exchange_rate():
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    try:
        df_recent = pd.read_sql_query(f'SELECT date FROM public.src_usd_{currency.lower()} ORDER BY date DESC '
                                      f'LIMIT 1', engine)
        recent = datetime.strptime(df_recent.iloc[0].iat[0], '%Y-%m-%d').date()
        date_diff = np.busday_count(recent, date.today()) - 1
        print(f'{date_diff} business days behind')
        if date_diff <= 1:
            print('No need to pull data')
            return
        elif date_diff <= 100:
            output_size = 'compact'
            print('Pulling 100 rows')
        else:
            output_size = 'full'
            print('Pulling all available data')
        table_exists = True
    except exc.ProgrammingError as e:  # to catch an exception, when the table does not exist
        print(e)
        output_size = 'full'
        table_exists = False
        print('Pulling all available data')
    except:                            # to catch other exceptions
        output_size = 'full'
        table_exists = False
        print('Pulling all available data')

    url = f'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol={currency}' \
          f'&outputsize={output_size}&apikey={os.getenv("ALPHAVANTAGE_API_KEY")}'
    r = requests.get(url)
    data = r.json()
    # convert time series to df, then transpose df and reverse rows
    df_exchange_rate = pd.DataFrame(data["Time Series FX (Daily)"]).transpose().iloc[::-1]

    if table_exists:
        df_exchange_rate = df_exchange_rate.tail(date_diff)  # keep only those rows, that are missing from the database
    for col in df_exchange_rate:
        df_exchange_rate[col] = pd.to_numeric(df_exchange_rate[col])
    df_exchange_rate.to_sql(f'src_usd_{currency.lower()}', engine, if_exists='append', index=True, index_label='date')
    if not table_exists:
        with engine.connect() as con:
            con.execute(f'ALTER TABLE src_usd_{currency.lower()} ADD PRIMARY KEY (date);')


# transform and load task
# create a new dataframe consisting of close price (in USD) of a stock with a given symbol and close currency exchange
# rate, then calculate the stock price in the choosen currency and add it to the created dataframe and then load
# this dataframe into a database
@task()
def calc_load_daily_price_other_ccy():
    time.sleep(5)  # wait 5 seconds to allow database to update tables after extract tasks
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df_price_usd = pd.read_sql_query(f'SELECT * FROM public.src_{symbol.lower()}_price_usd ', engine, index_col="date")
    df_exchange_rate = pd.read_sql_query(f'SELECT * FROM public.src_usd_{currency.lower()}', engine, index_col="date")

    df_price_usd.drop(['1. open', '2. high', '3. low', '5. adjusted close',
                       '6. volume', '7. dividend amount', '8. split coefficient'],
                      axis=1, inplace=True)
    df_price_usd.rename(columns={'4. close': 'closePriceUsd'}, inplace=True)

    df_exchange_rate.drop(['1. open', '2. high', '3. low'], axis=1, inplace=True)
    df_exchange_rate.rename(columns={'4. close': 'closeRate'}, inplace=True)

    df_price_other_ccy = df_price_usd.join(df_exchange_rate)
    df_price_other_ccy[f'closePrice{currency.title()}'] = \
        df_price_other_ccy['closePriceUsd'] * df_price_other_ccy['closeRate']

    df_price_other_ccy[f'closePrice{currency.title()}'] = round(df_price_other_ccy[f'closePrice{currency.title()}'], 2)

    df_price_other_ccy.to_sql(f'prd_{symbol.lower()}_price_{currency.lower()}', engine, if_exists='replace', index=True)


with DAG(dag_id="alphavantage_etl_dag", schedule_interval="0 0 * * 2-6", start_date=datetime(2022, 12, 1),
         catchup=False, tags=["alphavantage"]) as dag:

    symbol = "SPY"
    currency = "PLN"

    # extract
    with TaskGroup("extract_load_src", tooltip="Extract and load symbol price in USD and currency exchange rate") \
            as extract_load_src:
        src_daily_price = get_daily_price()
        src_daily_exchange_rate = get_daily_exchange_rate()
        # order
        [src_daily_price, src_daily_exchange_rate]

    # transform and load
    transform_load = calc_load_daily_price_other_ccy()

    # order
    extract_load_src >> transform_load

import os
import requests
import pandas as pd
from datetime import date
import numpy as np
from retry import retry
from sqlalchemy import inspect, text
from constants import *
import pandas_market_calendars as mcal


def get_recent_row_date(db_con, table_name):
    """Get and then return the date from the most recent row in a given table."""
    recent = db_con.execute(text(f'SELECT date '
                                 f'FROM public.{table_name} '
                                 f'ORDER BY date DESC '
                                 f'LIMIT 1'))

    return recent.fetchone()[0]


def pull_data_from_api(params):
    """Get and then return data from Alpha Vantage API with given parameters."""
    url = 'https://www.alphavantage.co/query'
    r = requests.get(url=url, params=params)
    data = r.json()
    return data


def load_data_to_db(engine, db_con, df, table_name, table_exists):
    """Load provided data into a database."""
    df.to_sql(table_name,
              engine,
              if_exists='append',
              index=True,
              index_label='date')
    if not table_exists:
        db_con.execute(text(f'ALTER TABLE {table_name} ADD PRIMARY KEY (date);'))


def get_daily_price(engine):
    """Extract from Alpha Vantage API and then load into a database daily security price data in USD."""
    with engine.connect() as db_con:
        if inspect(engine).has_table(SECURITY_TABLE):
            table_exists = True

            recent = get_recent_row_date(db_con, SECURITY_TABLE)
            # calculates business day difference between the last database record's date and yesterday's date
            # taking into account holidays when NYSE was closed
            nyse = mcal.get_calendar('NYSE')
            date_diff = np.busday_count(recent, date.today(), holidays=nyse.holidays().holidays) - 1
            print(f'Table {SECURITY_TABLE}: {date_diff} business day(s) missing')

            if date_diff <= 0:
                print('No need to pull data')
                return
            elif date_diff <= 100:
                output_size = 'compact'  # pulls only 100 most recent records
                print('Pulling 100 rows')
            else:
                output_size = 'full'  # pulls all records
                print('Pulling all available data')

        else:
            table_exists = False
            output_size = 'full'
            print('Pulling all available data')

        params = {'function': 'TIME_SERIES_DAILY',
                  'symbol': SYMBOL,
                  'outputsize': output_size,
                  'apikey': str(os.getenv('ALPHAVANTAGE_API_KEY'))}
        data = pull_data_from_api(params)

        # convert time series to df then transpose df and reverse rows
        df_price_usd = pd.DataFrame(data["Time Series (Daily)"]).transpose().iloc[::-1]

        if table_exists:
            df_price_usd = df_price_usd.tail(date_diff)  # keep only those rows, that are missing from the database
        df_price_usd = df_price_usd.apply(pd.to_numeric)
        df_price_usd.index = pd.to_datetime(df_price_usd.index).date

        load_data_to_db(engine, db_con, df_price_usd, SECURITY_TABLE, table_exists)


def get_daily_exchange_rate(engine):
    """Extract from AV API and then load into a database daily exchange rate data between USD and a given currency."""
    with engine.connect() as db_con:
        if inspect(engine).has_table(CURRENCY_TABLE):
            table_exists = True

            recent = get_recent_row_date(db_con, CURRENCY_TABLE)
            # calculates business day difference between the last database record's date and yesterday's date
            # taking into account holidays when NYSE was closed
            date_diff = np.busday_count(recent, date.today())
            print(f'Table {CURRENCY_TABLE}: {date_diff - 1} business day(s) missing')

            if date_diff <= 1:
                print('No need to pull data')
                return
            elif date_diff <= 100:
                output_size = 'compact'  # pulls only 100 most recent records
                print('Pulling 100 rows')
            else:
                output_size = 'full'  # pulls all records
                print('Pulling all available data')

        else:
            table_exists = False
            output_size = 'full'
            print('Pulling all available data')

        params = {'function': 'FX_DAILY',
                  'from_symbol': 'USD',
                  'to_symbol': CURRENCY,
                  'outputsize': output_size,
                  'apikey': str(os.getenv('ALPHAVANTAGE_API_KEY'))}
        data = pull_data_from_api(params)

        # convert time series to df, then transpose df and reverse rows
        df_exchange_rate = pd.DataFrame(data["Time Series FX (Daily)"]).transpose().iloc[::-1]

        if date.today().weekday() < 5:  # checks if today is any day between Monday and Friday
            if table_exists:
                # keep only those rows, that are missing from the database
                df_exchange_rate = df_exchange_rate.tail(date_diff)
            df_exchange_rate.drop(df_exchange_rate.tail(1).index, inplace=True)  # drop the data from the current day
        else:
            if table_exists:
                df_exchange_rate = df_exchange_rate.tail(date_diff-1)

        df_exchange_rate = df_exchange_rate.apply(pd.to_numeric)
        df_exchange_rate.index = pd.to_datetime(df_exchange_rate.index).date

        load_data_to_db(engine, db_con, df_exchange_rate, CURRENCY_TABLE, table_exists)


@retry(Exception, tries=5, delay=1)
def calc_load_daily_price_other_ccy(engine):
    """Calculate and then load into a database daily security price data in a given currency."""
    with engine.connect() as db_con:
        if inspect(engine).has_table(COMPARISON_TABLE):
            table_exists = True

            recent = get_recent_row_date(engine, COMPARISON_TABLE)
            # calculates business day difference between the last database record's date and yesterday's date
            # taking into account holidays when NYSE was closed
            nyse = mcal.get_calendar('NYSE')
            nyse_date_diff = np.busday_count(recent, date.today(), holidays=nyse.holidays().holidays) - 1
            # calculates business day difference between the last database record's date and yesterday's date
            forex_date_diff = np.busday_count(recent, date.today()) - 1

            if nyse_date_diff == 0:
                print(f'Table {COMPARISON_TABLE} is up to date')
                return

            print(f'Table {COMPARISON_TABLE} is not up to date')
            print(f'Pulling {nyse_date_diff} row(s) from {SECURITY_TABLE}')
            print(f'Pulling {forex_date_diff} row(s) from {CURRENCY_TABLE}')

            df_price_usd = pd.read_sql_query(sql=text(f'SELECT date, "4. close" '
                                                      f'FROM public.{SECURITY_TABLE} '
                                                      f'ORDER BY date DESC '
                                                      f'LIMIT {nyse_date_diff}'),
                                             con=db_con,
                                             index_col="date")
            df_exchange_rate = pd.read_sql_query(sql=text(f'SELECT date, "4. close" '
                                                          f'FROM public.{CURRENCY_TABLE} '
                                                          f'ORDER BY date DESC '
                                                          f'LIMIT {forex_date_diff}'),
                                                 con=db_con,
                                                 index_col="date")

        else:
            table_exists = False
            print(f'Pulling all rows from {SECURITY_TABLE} and {CURRENCY_TABLE}')

            df_price_usd = pd.read_sql_query(sql=text(f'SELECT date, "4. close" '
                                                      f'FROM public.{SECURITY_TABLE}'),
                                             con=db_con,
                                             index_col="date")
            df_exchange_rate = pd.read_sql_query(sql=text(f'SELECT date, "4. close" '
                                                          f'FROM public.{CURRENCY_TABLE}'),
                                                 con=db_con,
                                                 index_col="date")

        df_price_usd.rename(columns={'4. close': 'closePriceUsd'}, inplace=True)
        df_exchange_rate.rename(columns={'4. close': 'closeRate'}, inplace=True)

        df_price_other_ccy = df_price_usd.join(df_exchange_rate)
        df_price_other_ccy.dropna(inplace=True)
        df_price_other_ccy[f'closePrice{CURRENCY.title()}'] = \
            round(df_price_other_ccy['closePriceUsd'] * df_price_other_ccy['closeRate'], 2)

        load_data_to_db(engine, db_con, df_price_other_ccy, COMPARISON_TABLE, table_exists)

import os
import requests
import pandas as pd
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from datetime import datetime, date
import numpy as np
from retry import retry
import datapane as dp
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px


# extract tasks
# extract from alphavantage and load to local postgresql database daily price (in USD) of a given symbol
@task()
def get_daily_price():
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df_holidays = pd.read_sql_query(f'SELECT * FROM public.holidays', engine)
    # np array with dates from 01-01-2016 to 31-12-2024 when NYSE was or will be closed
    holidays = df_holidays.to_numpy(dtype='datetime64').flatten()
    try:
        df_recent = pd.read_sql_query(f'SELECT date FROM public.src_{symbol.lower()}_price_usd ORDER BY date DESC '
                                      f'LIMIT 1', engine)
        recent = df_recent.iloc[0].iat[0].date()
        # calculates business day difference between the last database record's date and yesterday's date taking into
        # account holidays when NYSE was closed
        date_diff = np.busday_count(recent, date.today(), holidays=holidays) - 1
        print(f'{date_diff} business day(s) behind')
        if date_diff <= 0:
            print('No need to pull data')
            return
        elif date_diff <= 100:
            output_size = 'compact'  # pulls only 100 most recent records
            print('Pulling 100 rows')
        else:
            output_size = 'full'  # pulls all records
            print('Pulling all available data')
        table_exists = True
    except Exception as e:  # to catch all exceptions
        print(e)
        output_size = 'full'
        table_exists = False  # making an assumption that table does not exist
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
    df_price_usd.index = pd.to_datetime(df_price_usd.index)

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
        recent = df_recent.iloc[0].iat[0].date()
        # calculates business day difference between the last database record's date and yesterday's date
        date_diff = np.busday_count(recent, date.today())
        print(f'{date_diff - 1} business day(s) behind')
        if date_diff <= 1:
            print('No need to pull data')
            return
        elif date_diff <= 100:
            output_size = 'compact'  # pulls only 100 most recent records
            print('Pulling 100 rows')
        else:
            output_size = 'full'  # pulls all records
            print('Pulling all available data')
        table_exists = True
    except Exception as e:  # to catch all exceptions
        print(e)
        output_size = 'full'
        table_exists = False  # making an assumption that table does not exist
        print('Pulling all available data')

    url = f'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol={currency}' \
          f'&outputsize={output_size}&apikey={os.getenv("ALPHAVANTAGE_API_KEY")}'
    r = requests.get(url)
    data = r.json()
    # convert time series to df, then transpose df and reverse rows
    df_exchange_rate = pd.DataFrame(data["Time Series FX (Daily)"]).transpose().iloc[::-1]

    if date.today().weekday() < 5:
        if table_exists:
            # keep only those rows, that are missing from the database
            df_exchange_rate = df_exchange_rate.tail(date_diff)
        df_exchange_rate.drop(df_exchange_rate.tail(1).index, inplace=True)  # drop the data from the current day
    else:
        if table_exists:
            df_exchange_rate = df_exchange_rate.tail(date_diff-1)

    for col in df_exchange_rate:
        df_exchange_rate[col] = pd.to_numeric(df_exchange_rate[col])
    df_exchange_rate.index = pd.to_datetime(df_exchange_rate.index)

    df_exchange_rate.to_sql(f'src_usd_{currency.lower()}', engine, if_exists='append', index=True, index_label='date')
    if not table_exists:
        with engine.connect() as con:
            con.execute(f'ALTER TABLE src_usd_{currency.lower()} ADD PRIMARY KEY (date);')


# transform and load task
# create a new dataframe consisting of close price (in USD) of a stock with a given symbol and close currency exchange
# rate, then calculate the stock price in the choosen currency and add it to the created dataframe and then load
# this dataframe into a database
@task()
@retry(Exception, tries=5, delay=1)
def calc_load_daily_price_other_ccy():
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df_holidays = pd.read_sql_query(f'SELECT * FROM public.holidays', engine)
    # np array with dates from 01-01-2016 to 31-12-2024 when NYSE was or will be closed
    holidays = df_holidays.to_numpy(dtype='datetime64').flatten()
    try:
        df_recent = pd.read_sql_query(f'SELECT date FROM public.prd_{symbol.lower()}_price_{currency.lower()} '
                                      f'ORDER BY date DESC LIMIT 1', engine)
        recent = df_recent.iloc[0].iat[0].date()
        # calculates business day difference between the last database record's date and yesterday's date taking into
        # account holidays when NYSE was closed
        nyse_date_diff = np.busday_count(recent, date.today(), holidays=holidays) - 1
        # calculates business day difference between the last database record's date and yesterday's date
        forex_date_diff = np.busday_count(recent, date.today()) - 1
        if nyse_date_diff == 0:
            print(f'Table prd_{symbol.lower()}_price_{currency.lower()} is up to date')
            return
        table_exists = True
        print(f'Pulling {nyse_date_diff} row(s) from src_{symbol.lower()}_price_usd')
        print(f'Pulling {forex_date_diff} row(s) from src_usd_{currency.lower()}')
    except Exception as e:  # to catch all exceptions
        print(e)
        table_exists = False  # making an assumption that table does not exist
        print(f'Pulling all rows from src_{symbol.lower()}_price_usd and src_usd_{currency.lower()}')

    if table_exists:
        # pulls only necessary rows
        df_price_usd = pd.read_sql_query(f'SELECT date, "4. close" '
                                         f'FROM public.src_{symbol.lower()}_price_usd ORDER BY date '
                                         f'DESC LIMIT {nyse_date_diff}', engine, index_col="date")
        df_exchange_rate = pd.read_sql_query(f'SELECT date, "4. close" '
                                             f'FROM public.src_usd_{currency.lower()} ORDER BY date '
                                             f'DESC LIMIT {forex_date_diff}', engine, index_col="date")
    else:
        # pulls all rows
        df_price_usd = pd.read_sql_query(f'SELECT date, "4. close" FROM public.src_{symbol.lower()}_price_usd ', engine,
                                         index_col="date")
        df_exchange_rate = pd.read_sql_query(f'SELECT date, "4. close" FROM public.src_usd_{currency.lower()}', engine,
                                             index_col="date")

    df_price_usd.rename(columns={'4. close': 'closePriceUsd'}, inplace=True)
    df_exchange_rate.rename(columns={'4. close': 'closeRate'}, inplace=True)

    df_price_other_ccy = df_price_usd.join(df_exchange_rate)
    df_price_other_ccy.dropna(inplace=True)
    df_price_other_ccy[f'closePrice{currency.title()}'] = \
        df_price_other_ccy['closePriceUsd'] * df_price_other_ccy['closeRate']

    df_price_other_ccy[f'closePrice{currency.title()}'] = round(df_price_other_ccy[f'closePrice{currency.title()}'], 2)

    df_price_other_ccy.to_sql(f'prd_{symbol.lower()}_price_{currency.lower()}', engine, if_exists='append', index=True,
                              index_label='date')
    if not table_exists:
        with engine.connect() as con:
            con.execute(f'ALTER TABLE prd_{symbol.lower()}_price_{currency.lower()} ADD PRIMARY KEY (date);')


# data visualization task
@task()
def visualize_data():
    os.system(f'datapane login --token={os.getenv("DATAPANE_TOKEN")}')
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df_price_usd = pd.read_sql_query(f'SELECT date, "1. open", "2. high", "3. low", "4. close" '
                                     f'FROM public.src_{symbol.lower()}_price_usd ORDER BY date DESC', engine)
    df_exchange_rate = pd.read_sql_query(f'SELECT * FROM public.src_usd_{currency.lower()} ORDER BY date DESC', engine)
    df_price_other_ccy = pd.read_sql_query(f'SELECT * FROM public.prd_{symbol.lower()}_price_{currency.lower()} '
                                           f'ORDER BY date DESC', engine)

    html_title = f'''
        <html>
            <style type='text/css'>
                #container {{
                    margin: auto;
                    text-align: center;
                    height: 50px;
                }}
                h1 {{
                    color:#444444;
                }}
            </style>
            <div id="container">
              <h1>{symbol.upper()} price report</h1>
            </div>
        </html>
        '''
    fig1_title = f'{symbol.upper()} price in USD'
    fig2_title = f'USD/{currency.upper()} exchange rate'
    fig3_title = f'{symbol.upper()} price in {currency.upper()} and USD'

    fig1a = go.Figure(data=[go.Candlestick(x=df_price_usd['date'],
                                           open=df_price_usd['1. open'],
                                           high=df_price_usd['2. high'],
                                           low=df_price_usd['3. low'],
                                           close=df_price_usd['4. close'])
                            ])
    fig1a.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Price'
    )

    fig1b = go.Figure(data=[go.Ohlc(x=df_price_usd['date'],
                                    open=df_price_usd['1. open'],
                                    high=df_price_usd['2. high'],
                                    low=df_price_usd['3. low'],
                                    close=df_price_usd['4. close'])
                            ])
    fig1b.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Price'
    )

    fig1c = px.line(df_price_usd, x='date', y='4. close')
    fig1c.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Close price'
    )

    fig2a = go.Figure(data=[go.Candlestick(x=df_exchange_rate['date'],
                                           open=df_exchange_rate['1. open'],
                                           high=df_exchange_rate['2. high'],
                                           low=df_exchange_rate['3. low'],
                                           close=df_exchange_rate['4. close'])
                            ])
    fig2a.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Exchange rate'
    )

    fig2b = go.Figure(data=[go.Ohlc(x=df_exchange_rate['date'],
                                    open=df_exchange_rate['1. open'],
                                    high=df_exchange_rate['2. high'],
                                    low=df_exchange_rate['3. low'],
                                    close=df_exchange_rate['4. close'])
                            ])
    fig2b.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Exchange rate'
    )

    fig2c = px.line(df_exchange_rate, x='date', y='4. close')
    fig2c.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Close exchange rate'
    )

    fig3 = make_subplots(specs=[[{'secondary_y': True}]])

    fig3.add_trace(
        go.Scatter(
            x=df_price_other_ccy['date'], y=df_price_other_ccy[f'closePrice{currency.title()}'],
            name=f'Close price in {currency.upper()}'
        ),
        secondary_y=False,
    )

    fig3.add_trace(
        go.Scatter(
            x=df_price_other_ccy['date'], y=df_price_other_ccy['closePriceUsd'],
            name='Close price in USD'
        ),
        secondary_y=True,
    )

    fig3.update_xaxes(title_text='Date')
    fig3.update_yaxes(title_text=f'Close price in {currency.upper()}', secondary_y=False)
    fig3.update_yaxes(title_text='Close price in USD', secondary_y=True)

    report = dp.Report(
        dp.HTML(html_title),
        dp.Text(fig1_title),
        dp.Select(
            blocks=[
                dp.Plot(fig1a, label='Candlestick chart'),
                dp.Plot(fig1b, label='OHLC chart'),
                dp.Plot(fig1c, label='Line chart')
            ]),
        dp.Text(fig2_title),
        dp.Select(
            blocks=[
                dp.Plot(fig2a, label='Candlestick chart'),
                dp.Plot(fig2b, label='OHLC chart'),
                dp.Plot(fig2c, label='Line chart')
            ]),
        dp.Text(fig3_title),
        dp.Plot(fig3),
        dp.Select(
            blocks=[
                dp.DataTable(df_price_usd, label=f'{symbol.upper()} price in USD'),
                dp.DataTable(df_exchange_rate, label=f'USD/{currency.upper()} exchange rate'),
                dp.DataTable(df_price_other_ccy, label=f'{symbol.upper()} price comparison in both currencies')
            ])
    )

    report.save('alphavantage-etl', open=True)
    report.upload(name='alphavantage-etl')


# this DAG will be triggered at midnight after every business day
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

    # visualize
    visualize = visualize_data()

    # order
    extract_load_src >> transform_load >> visualize

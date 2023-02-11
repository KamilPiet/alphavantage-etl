import os
import requests
import pandas as pd
from datetime import date
import numpy as np
from retry import retry
import datapane as dp
import plotly.graph_objects as go
from plotly.subplots import make_subplots

SYMBOL = "SPY"
CURRENCY = "PLN"


def get_holidays_np_array(engine):
    df_holidays = pd.read_sql_query(f'SELECT * FROM public.holidays', engine)
    # np array with dates from 01-01-2016 to 31-12-2024 when NYSE was or will be closed
    holidays = df_holidays.to_numpy(dtype='datetime64[D]').flatten()
    return holidays


def pull_data_from_api(url, params):
    r = requests.get(url=url, params=params)
    data = r.json()
    return data


def get_daily_price(engine):
    holidays = get_holidays_np_array(engine)
    try:
        df_recent = pd.read_sql_query(f'SELECT date '
                                      f'FROM public.src_{SYMBOL.lower()}_price_usd '
                                      f'ORDER BY date DESC '
                                      f'LIMIT 1',
                                      engine)
        recent = df_recent.iloc[0].iat[0].date()
        # calculates business day difference between the last database record's date and yesterday's date taking into
        # account holidays when NYSE was closed
        date_diff = np.busday_count(recent, date.today(), holidays=holidays) - 1
        print(f'Table src_{SYMBOL.lower()}_price_usd: {date_diff} business day(s) missing')
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

    url = 'https://www.alphavantage.co/query'
    params = {'function': 'TIME_SERIES_DAILY_ADJUSTED',
              'symbol': SYMBOL,
              'outputsize': output_size,
              'apikey': str(os.getenv('ALPHAVANTAGE_API_KEY'))}
    data = pull_data_from_api(url, params)

    # convert time series to df then transpose df and reverse rows
    df_price_usd = pd.DataFrame(data["Time Series (Daily)"]).transpose().iloc[::-1]

    if table_exists:
        df_price_usd = df_price_usd.tail(date_diff)  # keep only those rows, that are missing from the database
    for col in df_price_usd:
        df_price_usd[col] = pd.to_numeric(df_price_usd[col])
    df_price_usd.index = pd.to_datetime(df_price_usd.index)

    df_price_usd.to_sql(f'src_{SYMBOL.lower()}_price_usd',
                        engine,
                        if_exists='append',
                        index=True,
                        index_label='date')
    if not table_exists:
        with engine.connect() as con:
            con.execute(f'ALTER TABLE src_{SYMBOL.lower()}_price_usd '
                        f'ADD PRIMARY KEY (date);')


def get_daily_exchange_rate(engine):
    try:
        df_recent = pd.read_sql_query(f'SELECT date '
                                      f'FROM public.src_usd_{CURRENCY.lower()} '
                                      f'ORDER BY date DESC '
                                      f'LIMIT 1',
                                      engine)
        recent = df_recent.iloc[0].iat[0].date()
        # calculates business day difference between the last database record's date and yesterday's date
        date_diff = np.busday_count(recent, date.today())
        print(f'Table src_usd_{CURRENCY.lower()}: {date_diff - 1} business day(s) missing')
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

    url = 'https://www.alphavantage.co/query'
    params = {'function': 'FX_DAILY',
              'from_symbol': 'USD',
              'to_symbol': CURRENCY,
              'outputsize': output_size,
              'apikey': str(os.getenv('ALPHAVANTAGE_API_KEY'))}
    data = pull_data_from_api(url, params)
    
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

    df_exchange_rate.to_sql(f'src_usd_{CURRENCY.lower()}',
                            engine,
                            if_exists='append',
                            index=True,
                            index_label='date')
    if not table_exists:
        with engine.connect() as con:
            con.execute(f'ALTER TABLE src_usd_{CURRENCY.lower()} '
                        f'ADD PRIMARY KEY (date);')


@retry(Exception, tries=5, delay=1)
def calc_load_daily_price_other_ccy(engine):
    holidays = get_holidays_np_array(engine)
    try:
        df_recent = pd.read_sql_query(f'SELECT date '
                                      f'FROM public.prd_{SYMBOL.lower()}_price_{CURRENCY.lower()} '
                                      f'ORDER BY date DESC '
                                      f'LIMIT 1',
                                      engine)
        recent = df_recent.iloc[0].iat[0].date()
        # calculates business day difference between the last database record's date and yesterday's date taking into
        # account holidays when NYSE was closed
        nyse_date_diff = np.busday_count(recent, date.today(), holidays=holidays) - 1
        # calculates business day difference between the last database record's date and yesterday's date
        forex_date_diff = np.busday_count(recent, date.today()) - 1
        if nyse_date_diff == 0:
            print(f'Table prd_{SYMBOL.lower()}_price_{CURRENCY.lower()} is up to date')
            return
        table_exists = True
        print(f'Table prd_{SYMBOL.lower()}_price_{CURRENCY.lower()} is not up to date')
        print(f'Pulling {nyse_date_diff} row(s) from src_{SYMBOL.lower()}_price_usd')
        print(f'Pulling {forex_date_diff} row(s) from src_usd_{CURRENCY.lower()}')
    except Exception as e:  # to catch all exceptions
        print(e)
        table_exists = False  # making an assumption that table does not exist
        print(f'Pulling all rows from src_{SYMBOL.lower()}_price_usd and src_usd_{CURRENCY.lower()}')

    if table_exists:
        # pulls only necessary rows
        df_price_usd = pd.read_sql_query(f'SELECT date, "4. close" '
                                         f'FROM public.src_{SYMBOL.lower()}_price_usd '
                                         f'ORDER BY date DESC '
                                         f'LIMIT {nyse_date_diff}',
                                         engine,
                                         index_col="date")
        df_exchange_rate = pd.read_sql_query(f'SELECT date, "4. close" '
                                             f'FROM public.src_usd_{CURRENCY.lower()} '
                                             f'ORDER BY date DESC '
                                             f'LIMIT {forex_date_diff}',
                                             engine,
                                             index_col="date")
    else:
        # pulls all rows
        df_price_usd = pd.read_sql_query(f'SELECT date, "4. close" '
                                         f'FROM public.src_{SYMBOL.lower()}_price_usd ',
                                         engine,
                                         index_col="date")
        df_exchange_rate = pd.read_sql_query(f'SELECT date, "4. close" '
                                             f'FROM public.src_usd_{CURRENCY.lower()}',
                                             engine,
                                             index_col="date")

    df_price_usd.rename(columns={'4. close': 'closePriceUsd'}, inplace=True)
    df_exchange_rate.rename(columns={'4. close': 'closeRate'}, inplace=True)

    df_price_other_ccy = df_price_usd.join(df_exchange_rate)
    df_price_other_ccy.dropna(inplace=True)
    df_price_other_ccy[f'closePrice{CURRENCY.title()}'] = \
        df_price_other_ccy['closePriceUsd'] * df_price_other_ccy['closeRate']

    df_price_other_ccy[f'closePrice{CURRENCY.title()}'] = round(df_price_other_ccy[f'closePrice{CURRENCY.title()}'], 2)

    df_price_other_ccy.to_sql(f'prd_{SYMBOL.lower()}_price_{CURRENCY.lower()}',
                              engine,
                              if_exists='append',
                              index=True,
                              index_label='date')
    if not table_exists:
        with engine.connect() as con:
            con.execute(f'ALTER TABLE prd_{SYMBOL.lower()}_price_{CURRENCY.lower()} '
                        f'ADD PRIMARY KEY (date);')


def visualize_data(engine):
    print('Preparing data for the report...')
    df_price_usd = pd.read_sql_query(f'SELECT date, "1. open", "2. high", "3. low", "4. close" '
                                     f'FROM public.src_{SYMBOL.lower()}_price_usd '
                                     f'ORDER BY date DESC',
                                     engine)
    df_exchange_rate = pd.read_sql_query(f'SELECT * '
                                         f'FROM public.src_usd_{CURRENCY.lower()} '
                                         f'ORDER BY date DESC',
                                         engine)
    df_price_other_ccy = pd.read_sql_query(f'SELECT * '
                                           f'FROM public.prd_{SYMBOL.lower()}_price_{CURRENCY.lower()} '
                                           f'ORDER BY date DESC',
                                           engine)

    # the number of days over which the average is calculated
    sma_1 = 20
    sma_2 = 90

    df_price_usd['SMA_1'] = df_price_usd['4. close'].rolling(sma_1).mean().shift(-sma_1)
    df_price_usd['SMA_2'] = df_price_usd['4. close'].rolling(sma_2).mean().shift(-sma_2)

    df_exchange_rate['SMA_1'] = df_exchange_rate['4. close'].rolling(sma_1).mean().shift(-sma_1)
    df_exchange_rate['SMA_2'] = df_exchange_rate['4. close'].rolling(sma_2).mean().shift(-sma_2)

    df_price_other_ccy['SMA_1'] = \
        df_price_other_ccy[f'closePrice{CURRENCY.title()}'].rolling(sma_1).mean().shift(-sma_1)
    df_price_other_ccy['SMA_2'] = \
        df_price_other_ccy[f'closePrice{CURRENCY.title()}'].rolling(sma_2).mean().shift(-sma_2)

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
                  <h1>{SYMBOL.upper()} price report</h1>
                </div>
            </html>
            '''
    fig1_title = f'{SYMBOL.upper()} price in USD'
    fig2_title = f'USD/{CURRENCY.upper()} exchange rate'
    fig3_title = f'{SYMBOL.upper()} price in {CURRENCY.upper()} and USD'

    color_1 = '#0080FF'
    color_2 = '#FF8000'
    color_3 = '#00FF00'
    color_4 = '#0000FF'
    color_5 = '#FF0000'
    color_6 = '#007700'

    # price charts in USD
    fig1a = go.Figure(data=[go.Candlestick(x=df_price_usd['date'],
                                           open=df_price_usd['1. open'],
                                           high=df_price_usd['2. high'],
                                           low=df_price_usd['3. low'],
                                           close=df_price_usd['4. close'],
                                           name='Price'),
                            go.Scatter(x=df_price_usd['date'],
                                       y=df_price_usd['SMA_1'],
                                       name=f'SMA {sma_1}',
                                       line=dict(color=color_2, width=1)),
                            go.Scatter(x=df_price_usd['date'],
                                       y=df_price_usd['SMA_2'],
                                       name=f'SMA {sma_2}',
                                       line=dict(color=color_3, width=1)),
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
                                    close=df_price_usd['4. close'],
                                    name='Price'),
                            go.Scatter(x=df_price_usd['date'],
                                       y=df_price_usd['SMA_1'],
                                       name=f'SMA {sma_1}',
                                       line=dict(color=color_2, width=1)),
                            go.Scatter(x=df_price_usd['date'],
                                       y=df_price_usd['SMA_2'],
                                       name=f'SMA {sma_2}',
                                       line=dict(color=color_3, width=1)),
                            ])
    fig1b.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Price'
    )

    fig1c = go.Figure(data=[go.Scatter(x=df_price_usd['date'],
                                       y=df_price_usd['4. close'],
                                       name='Price',
                                       line=dict(color=color_1, width=2)),
                            go.Scatter(x=df_price_usd['date'],
                                       y=df_price_usd['SMA_1'],
                                       name=f'SMA {sma_1}',
                                       line=dict(color=color_2, width=1)),
                            go.Scatter(x=df_price_usd['date'],
                                       y=df_price_usd['SMA_2'],
                                       name=f'SMA {sma_2}',
                                       line=dict(color=color_3, width=1)),
                            ])
    fig1c.update_layout(
        xaxis_title='Date',
        yaxis_title='Close price'
    )

    # exchange rate charts
    fig2a = go.Figure(data=[go.Candlestick(x=df_exchange_rate['date'],
                                           open=df_exchange_rate['1. open'],
                                           high=df_exchange_rate['2. high'],
                                           low=df_exchange_rate['3. low'],
                                           close=df_exchange_rate['4. close'],
                                           name='Price'),
                            go.Scatter(x=df_exchange_rate['date'],
                                       y=df_exchange_rate['SMA_1'],
                                       name=f'SMA {sma_1}',
                                       line=dict(color=color_2, width=1)),
                            go.Scatter(x=df_exchange_rate['date'],
                                       y=df_exchange_rate['SMA_2'],
                                       name=f'SMA {sma_2}',
                                       line=dict(color=color_3, width=1)),
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
                                    close=df_exchange_rate['4. close'],
                                    name='Price'),
                            go.Scatter(x=df_exchange_rate['date'],
                                       y=df_exchange_rate['SMA_1'],
                                       name=f'SMA {sma_1}',
                                       line=dict(color=color_2, width=1)),
                            go.Scatter(x=df_exchange_rate['date'],
                                       y=df_exchange_rate['SMA_2'],
                                       name=f'SMA {sma_2}',
                                       line=dict(color=color_3, width=1)),
                            ])
    fig2b.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Exchange rate'
    )

    fig2c = go.Figure(data=[go.Scatter(x=df_exchange_rate['date'],
                                       y=df_exchange_rate['4. close'],
                                       name='Price',
                                       line=dict(color=color_1, width=2)),
                            go.Scatter(x=df_exchange_rate['date'],
                                       y=df_exchange_rate['SMA_1'],
                                       name=f'SMA {sma_1}',
                                       line=dict(color=color_2, width=1)),
                            go.Scatter(x=df_exchange_rate['date'],
                                       y=df_exchange_rate['SMA_2'],
                                       name=f'SMA {sma_2}',
                                       line=dict(color=color_3, width=1)),
                            ])
    fig2c.update_layout(
        xaxis_title='Date',
        yaxis_title='Close exchange rate'
    )

    # price comparison chart
    fig3 = make_subplots(specs=[[{'secondary_y': True}]])

    fig3.add_trace(
        go.Scatter(
            x=df_price_other_ccy['date'],
            y=df_price_other_ccy[f'closePrice{CURRENCY.title()}'],
            name=f'Close price in {CURRENCY.upper()}',
            line=dict(color=color_1, width=2)
        ),
        secondary_y=False,
    )
    fig3.add_trace(
        go.Scatter(
            x=df_price_other_ccy['date'],
            y=df_price_other_ccy['SMA_1'],
            name=f'SMA {sma_1} of close price in {CURRENCY.upper()}',
            line=dict(color=color_2, width=1)
        ),
        secondary_y=False,
    )
    fig3.add_trace(
        go.Scatter(
            x=df_price_other_ccy['date'],
            y=df_price_other_ccy['SMA_2'],
            name=f'SMA {sma_2} of close price in {CURRENCY.upper()}',
            line=dict(color=color_3, width=1)
        ),
        secondary_y=False,
    )
    fig3.add_trace(
        go.Scatter(
            x=df_price_other_ccy['date'],
            y=df_price_other_ccy['closePriceUsd'],
            name='Close price in USD',
            line=dict(color=color_4, width=2)
        ),
        secondary_y=True,
    )
    fig3.add_trace(
        go.Scatter(
            x=df_price_other_ccy['date'],
            y=df_price_usd['SMA_1'],
            name=f'SMA {sma_1} of close price in USD',
            line=dict(color=color_5, width=1)
        ),
        secondary_y=True,
    )
    fig3.add_trace(
        go.Scatter(
            x=df_price_other_ccy['date'],
            y=df_price_usd['SMA_2'],
            name=f'SMA {sma_2} of close price in USD',
            line=dict(color=color_6, width=1)
        ),
        secondary_y=True,
    )

    fig3.update_xaxes(title_text='Date')
    fig3.update_yaxes(title_text=f'Close price in {CURRENCY.upper()}', secondary_y=False)
    fig3.update_yaxes(title_text='Close price in USD', secondary_y=True)

    print('Logging in to Datapane...')
    os.system(f'datapane login --token={os.getenv("DATAPANE_TOKEN")}')

    # datapane report
    print('Creating the report...')
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
                dp.DataTable(df_price_usd.iloc[:, 0:5], label=f'{SYMBOL.upper()} price in USD'),
                dp.DataTable(df_exchange_rate.iloc[:, 0:5], label=f'USD/{CURRENCY.upper()} exchange rate'),
                dp.DataTable(df_price_other_ccy.iloc[:, 0:4],
                             label=f'{SYMBOL.upper()} price comparison in both currencies')
            ])
    )

    report.save('/tmp/alphavantage-etl', open=True)
    report.upload(name='alphavantage-etl')

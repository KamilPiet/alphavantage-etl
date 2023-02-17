import os
import requests
import pandas as pd
from datetime import date
import numpy as np
from retry import retry
import datapane as dp
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import inspect

SYMBOL = "SPY"
CURRENCY = "PLN"

# names of the database tables
SECURITY_TABLE = f'src_{SYMBOL.lower()}_price_usd'
CURRENCY_TABLE = f'src_usd_{CURRENCY.lower()}'
COMPARISON_TABLE = f'prd_{SYMBOL.lower()}_price_{CURRENCY.lower()}'

# a tuple of colors used in the price report
COLORS = ('#0080FF', '#FF8000', '#00FF00', '#0000FF', '#FF0000', '#007700')

# a tuple of window sizes of simple moving averages used in the price report
SMA = (20, 90)


def get_recent_row_date(engine, table_name):
    df_recent = pd.read_sql_query(f'SELECT date '
                                  f'FROM public.{table_name} '
                                  f'ORDER BY date DESC '
                                  f'LIMIT 1',
                                  engine)
    recent = df_recent.iloc[0].iat[0]
    return recent


def get_holidays_np_array(engine):
    df_holidays = pd.read_sql_query(f'SELECT * FROM public.holidays', engine)
    # np array with dates from 01-01-2016 to 31-12-2024 when NYSE was or will be closed
    holidays = df_holidays.to_numpy(dtype='datetime64[D]').flatten()
    return holidays


def pull_data_from_api(params):
    url = 'https://www.alphavantage.co/query'
    r = requests.get(url=url, params=params)
    data = r.json()
    return data


def load_data_to_db(engine, df, table_name, table_exists):
    df.to_sql(table_name,
              engine,
              if_exists='append',
              index=True,
              index_label='date')
    if not table_exists:
        with engine.connect() as con:
            con.execute(f'ALTER TABLE {table_name} '
                        f'ADD PRIMARY KEY (date);')


def get_daily_price(engine):
    if inspect(engine).has_table(SECURITY_TABLE):
        table_exists = True

        recent = get_recent_row_date(engine, SECURITY_TABLE)
        holidays = get_holidays_np_array(engine)
        # calculates business day difference between the last database record's date and yesterday's date taking into
        # account holidays when NYSE was closed
        date_diff = np.busday_count(recent, date.today(), holidays=holidays) - 1
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

    params = {'function': 'TIME_SERIES_DAILY_ADJUSTED',
              'symbol': SYMBOL,
              'outputsize': output_size,
              'apikey': str(os.getenv('ALPHAVANTAGE_API_KEY'))}
    data = pull_data_from_api(params)

    # convert time series to df then transpose df and reverse rows
    df_price_usd = pd.DataFrame(data["Time Series (Daily)"]).transpose().iloc[::-1]

    if table_exists:
        df_price_usd = df_price_usd.tail(date_diff)  # keep only those rows, that are missing from the database
    for col in df_price_usd:
        df_price_usd[col] = pd.to_numeric(df_price_usd[col])
    df_price_usd.index = pd.to_datetime(df_price_usd.index).date

    load_data_to_db(engine, df_price_usd, SECURITY_TABLE, table_exists)


def get_daily_exchange_rate(engine):
    if inspect(engine).has_table(CURRENCY_TABLE):
        table_exists = True

        recent = get_recent_row_date(engine, CURRENCY_TABLE)
        # calculates business day difference between the last database record's date and yesterday's date taking into
        # account holidays when NYSE was closed
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
    df_exchange_rate.index = pd.to_datetime(df_exchange_rate.index).date

    load_data_to_db(engine, df_exchange_rate, CURRENCY_TABLE, table_exists)


@retry(Exception, tries=5, delay=1)
def calc_load_daily_price_other_ccy(engine):
    if inspect(engine).has_table(COMPARISON_TABLE):
        table_exists = True

        recent = get_recent_row_date(engine, COMPARISON_TABLE)
        holidays = get_holidays_np_array(engine)
        # calculates business day difference between the last database record's date and yesterday's date taking into
        # account holidays when NYSE was closed
        nyse_date_diff = np.busday_count(recent, date.today(), holidays=holidays) - 1
        # calculates business day difference between the last database record's date and yesterday's date
        forex_date_diff = np.busday_count(recent, date.today()) - 1

        if nyse_date_diff == 0:
            print(f'Table {COMPARISON_TABLE} is up to date')
            return

        print(f'Table {COMPARISON_TABLE} is not up to date')
        print(f'Pulling {nyse_date_diff} row(s) from {SECURITY_TABLE}')
        print(f'Pulling {forex_date_diff} row(s) from {CURRENCY_TABLE}')

        df_price_usd = pd.read_sql_query(f'SELECT date, "4. close" '
                                         f'FROM public.{SECURITY_TABLE} '
                                         f'ORDER BY date DESC '
                                         f'LIMIT {nyse_date_diff}',
                                         engine,
                                         index_col="date")
        df_exchange_rate = pd.read_sql_query(f'SELECT date, "4. close" '
                                             f'FROM public.{CURRENCY_TABLE} '
                                             f'ORDER BY date DESC '
                                             f'LIMIT {forex_date_diff}',
                                             engine,
                                             index_col="date")

    else:
        table_exists = False
        print(f'Pulling all rows from {SECURITY_TABLE} and {CURRENCY_TABLE}')

        df_price_usd = pd.read_sql_query(f'SELECT date, "4. close" '
                                         f'FROM public.{SECURITY_TABLE} ',
                                         engine,
                                         index_col="date")
        df_exchange_rate = pd.read_sql_query(f'SELECT date, "4. close" '
                                             f'FROM public.{CURRENCY_TABLE}',
                                             engine,
                                             index_col="date")

    df_price_usd.rename(columns={'4. close': 'closePriceUsd'}, inplace=True)
    df_exchange_rate.rename(columns={'4. close': 'closeRate'}, inplace=True)

    df_price_other_ccy = df_price_usd.join(df_exchange_rate)
    df_price_other_ccy.dropna(inplace=True)
    df_price_other_ccy[f'closePrice{CURRENCY.title()}'] = \
        df_price_other_ccy['closePriceUsd'] * df_price_other_ccy['closeRate']

    df_price_other_ccy[f'closePrice{CURRENCY.title()}'] = round(df_price_other_ccy[f'closePrice{CURRENCY.title()}'], 2)

    load_data_to_db(engine, df_price_other_ccy, COMPARISON_TABLE, table_exists)


def create_fig(df, fig_type):
    if fig_type == 'Candlestick':
        fig = go.Figure(data=[go.Candlestick(x=df['date'],
                                             open=df['1. open'],
                                             high=df['2. high'],
                                             low=df['3. low'],
                                             close=df['4. close'],
                                             name='Price'),
                              go.Scatter(x=df['date'],
                                         y=df['SMA_1'],
                                         name=f'SMA {SMA[0]}',
                                         line=dict(color=COLORS[1], width=1)),
                              go.Scatter(x=df['date'],
                                         y=df['SMA_2'],
                                         name=f'SMA {SMA[1]}',
                                         line=dict(color=COLORS[2], width=1)),
                              ])
    elif fig_type == 'Ohlc':
        fig = go.Figure(data=[go.Ohlc(x=df['date'],
                                      open=df['1. open'],
                                      high=df['2. high'],
                                      low=df['3. low'],
                                      close=df['4. close'],
                                      name='Price'),
                              go.Scatter(x=df['date'],
                                         y=df['SMA_1'],
                                         name=f'SMA {SMA[0]}',
                                         line=dict(color=COLORS[1], width=1)),
                              go.Scatter(x=df['date'],
                                         y=df['SMA_2'],
                                         name=f'SMA {SMA[1]}',
                                         line=dict(color=COLORS[2], width=1)),
                              ])
    else:
        fig = go.Figure(data=[go.Scatter(x=df['date'],
                                         y=df['4. close'],
                                         name='Price',
                                         line=dict(color=COLORS[0], width=2)),
                              go.Scatter(x=df['date'],
                                         y=df['SMA_1'],
                                         name=f'SMA {SMA[0]}',
                                         line=dict(color=COLORS[1], width=1)),
                              go.Scatter(x=df['date'],
                                         y=df['SMA_2'],
                                         name=f'SMA {SMA[1]}',
                                         line=dict(color=COLORS[2], width=1)),
                              ])

    fig.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Price'
    )

    return fig


def add_subplots(fig, df, columns, subplot_names, first_subplot_number, secondary_y):
    if len(columns) == len(subplot_names):
        i = first_subplot_number
        for col, name in zip(columns, subplot_names):
            # to highlight the first subplot 
            if i == first_subplot_number:
                width = 2
            else:
                width = 1

            fig.add_trace(
                go.Scatter(
                    x=df['date'],
                    y=df[col],
                    name=name,
                    line=dict(color=COLORS[i % len(COLORS)], width=width)
                ),
                secondary_y=secondary_y,
            )
            i = i + 1

    else:
        print("The number of subplot names doesn't match the number of columns")

    return fig


def visualize_data(engine):
    print('Preparing data for the report...')
    
    df_price_usd = pd.read_sql_query(f'SELECT date, "1. open", "2. high", "3. low", "4. close" '
                                     f'FROM public.{SECURITY_TABLE} '
                                     f'ORDER BY date DESC',
                                     engine)
    df_exchange_rate = pd.read_sql_query(f'SELECT * '
                                         f'FROM public.{CURRENCY_TABLE} '
                                         f'ORDER BY date DESC',
                                         engine)
    df_price_other_ccy = pd.read_sql_query(f'SELECT * '
                                           f'FROM public.{COMPARISON_TABLE} '
                                           f'ORDER BY date DESC',
                                           engine)

    df_price_usd['SMA_1'] = df_price_usd['4. close'].rolling(SMA[0]).mean().shift(-SMA[0])
    df_price_usd['SMA_2'] = df_price_usd['4. close'].rolling(SMA[1]).mean().shift(-SMA[1])

    df_exchange_rate['SMA_1'] = df_exchange_rate['4. close'].rolling(SMA[0]).mean().shift(-SMA[0])
    df_exchange_rate['SMA_2'] = df_exchange_rate['4. close'].rolling(SMA[1]).mean().shift(-SMA[1])

    df_price_other_ccy['SMA_1'] = \
        df_price_other_ccy[f'closePrice{CURRENCY.title()}'].rolling(SMA[0]).mean().shift(-SMA[0])
    df_price_other_ccy['SMA_2'] = \
        df_price_other_ccy[f'closePrice{CURRENCY.title()}'].rolling(SMA[1]).mean().shift(-SMA[1])

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

    # price charts in USD
    fig1a = create_fig(df_price_usd, 'Candlestick')
    fig1b = create_fig(df_price_usd, 'Ohlc')
    fig1c = create_fig(df_price_usd, 'Line')

    # exchange rate charts
    fig2a = create_fig(df_exchange_rate, 'Candlestick')
    fig2b = create_fig(df_exchange_rate, 'Ohlc')
    fig2c = create_fig(df_exchange_rate, 'Line')

    # price comparison chart
    fig3 = make_subplots(specs=[[{'secondary_y': True}]])

    # a list of column names to be added to the plot
    columns_price_other_ccy = (f'closePrice{CURRENCY.title()}',
                               'SMA_1',
                               'SMA_2')

    # a list of plot names
    subplot_names_price_other_ccy = (f'Close price in {CURRENCY.upper()}',
                                     f'SMA {SMA[0]} of close price in {CURRENCY.upper()}',
                                     f'SMA {SMA[1]} of close price in {CURRENCY.upper()}')

    fig3 = add_subplots(fig3, df_price_other_ccy, columns_price_other_ccy, subplot_names_price_other_ccy, 0, False)

    # a list of column names to be added to the plot
    columns_price_usd = ('4. close',
                         'SMA_1',
                         'SMA_2')

    # a list of plot names
    subplot_names_price_usd = (f'Close price in USD',
                               f'SMA {SMA[0]} of close price in USD',
                               f'SMA {SMA[1]} of close price in USD')

    fig3 = add_subplots(fig3, df_price_usd, columns_price_usd, subplot_names_price_usd, 3, True)

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

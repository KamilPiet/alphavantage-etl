import os
import pandas as pd
import datapane as dp
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from constants import *


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

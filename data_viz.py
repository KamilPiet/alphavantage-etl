import pandas as pd
import datapane as dp
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from constants import *
from sqlalchemy import text


class ComparisonFigure(go.Figure):
    """Figure to compare two or more subplots."""

    subplot_num = 0

    def __init__(self):
        super().__init__(make_subplots(specs=[[{'secondary_y': True}]]))

    def add_subplots(self, df, subplot_names, secondary_y):
        """Help with adding multiple subplots at once."""
        i = self.subplot_num
        for col in subplot_names:
            # to highlight the main subplot
            if i == self.subplot_num:
                width = 2
            else:
                width = 1

            self.add_trace(
                go.Scatter(
                    x=df['date'],
                    y=df[col],
                    name=subplot_names[col],
                    line=dict(color=COLORS[i % len(COLORS)], width=width)
                ),
                secondary_y=secondary_y,
            )
            i = i + 1

        self.subplot_num = i


def create_fig(df, plot_type):
    """Create and then return a plot of a given type togheter with two plots with simple moving averages."""
    if plot_type == 'Candlestick':
        fig = go.Figure(data=go.Candlestick(x=df['date'],
                                            open=df['1. open'],
                                            high=df['2. high'],
                                            low=df['3. low'],
                                            close=df['4. close'],
                                            name='Price'))
    elif plot_type == 'Ohlc':
        fig = go.Figure(data=go.Ohlc(x=df['date'],
                                     open=df['1. open'],
                                     high=df['2. high'],
                                     low=df['3. low'],
                                     close=df['4. close'],
                                     name='Price'))
    else:
        fig = go.Figure(data=go.Scatter(x=df['date'],
                                        y=df['4. close'],
                                        name='Price',
                                        line=dict(color=COLORS[0], width=2)))

    fig.add_scatter(x=df['date'],
                    y=df['SMA_1'],
                    name=f'SMA {SMA[0]}',
                    line=dict(color=COLORS[1], width=1))
    fig.add_scatter(x=df['date'],
                    y=df['SMA_2'],
                    name=f'SMA {SMA[1]}',
                    line=dict(color=COLORS[2], width=1))

    fig.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title='Date',
        yaxis_title='Price'
    )

    return fig


def visualize_data(engine):
    """Pull the necessary data from the database, calculate SMAs, prepare a datapane report and then upload it."""
    print('Preparing data for the report...')

    with engine.connect() as db_con:

        df_price_usd = pd.read_sql_query(sql=text(f'SELECT date, "1. open", "2. high", "3. low", "4. close" '
                                                  f'FROM public.{SECURITY_TABLE} '
                                                  f'ORDER BY date DESC'),
                                         con=db_con)
        df_exchange_rate = pd.read_sql_query(sql=text(f'SELECT * '
                                                      f'FROM public.{CURRENCY_TABLE} '
                                                      f'ORDER BY date DESC'),
                                             con=db_con)
        df_price_other_ccy = pd.read_sql_query(sql=text(f'SELECT * '
                                                        f'FROM public.{COMPARISON_TABLE} '
                                                        f'ORDER BY date DESC'),
                                               con=db_con)

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
    fig3 = ComparisonFigure()

    # a dictionary of subplot names to be added to the plot
    subplot_names_price_other_ccy = {f'closePrice{CURRENCY.title()}': f'Close price in {CURRENCY.upper()}',
                                     'SMA_1': f'SMA {SMA[0]} of close price in {CURRENCY.upper()}',
                                     'SMA_2': f'SMA {SMA[1]} of close price in {CURRENCY.upper()}'}

    fig3.add_subplots(df_price_other_ccy, subplot_names_price_other_ccy, False)

    # a dictionary of subplot names to be added to the plot
    subplot_names_price_usd = {'4. close': f'Close price in USD',
                               'SMA_1': f'SMA {SMA[0]} of close price in USD',
                               'SMA_2': f'SMA {SMA[1]} of close price in USD'}

    fig3.add_subplots(df_price_usd, subplot_names_price_usd, True)

    fig3.update_xaxes(title_text='Date')
    fig3.update_yaxes(title_text=f'Close price in {CURRENCY.upper()}', secondary_y=False)
    fig3.update_yaxes(title_text='Close price in USD', secondary_y=True)

    # datapane report
    print('Creating the report...')
    report = dp.App(
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

    return report

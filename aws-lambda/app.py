import os
from sqlalchemy import create_engine
from retry import retry

os.environ['XDG_CONFIG_HOME'] = '/tmp'
import av_etl

SYMBOL = "SPY"
CURRENCY = "PLN"

DB_LOGIN = os.getenv('DB_LOGIN')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')


def get_daily_price():
    engine = create_engine(f'postgresql://{DB_LOGIN}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    av_etl.get_daily_price(engine)


def get_daily_exchange_rate():
    engine = create_engine(f'postgresql://{DB_LOGIN}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    av_etl.get_daily_exchange_rate(engine)


@retry(Exception, tries=5, delay=1)
def calc_load_daily_price_other_ccy():
    engine = create_engine(f'postgresql://{DB_LOGIN}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    av_etl.calc_load_daily_price_other_ccy(engine)


def visualize_data():
    engine = create_engine(f'postgresql://{DB_LOGIN}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    av_etl.visualize_data(engine)


def handler(event, context):
    get_daily_price()
    get_daily_exchange_rate()
    calc_load_daily_price_other_ccy()
    visualize_data()

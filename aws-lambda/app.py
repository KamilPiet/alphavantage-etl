import os
from sqlalchemy import create_engine
import av_etl

os.environ['XDG_CONFIG_HOME'] = '/tmp'
import data_viz


def create_sql_engine():
    db_login = os.getenv('DB_LOGIN')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    engine = create_engine(f'postgresql://{db_login}:{db_password}@{db_host}:{db_port}/{db_name}')
    return engine


def handler(event, context):
    engine = create_sql_engine()
    av_etl.get_daily_price(engine)
    av_etl.get_daily_exchange_rate(engine)
    av_etl.calc_load_daily_price_other_ccy(engine)
    data_viz.visualize_data(engine)

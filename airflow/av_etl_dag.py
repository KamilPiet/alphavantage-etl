from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from datetime import datetime, timezone
import av_etl


def create_sql_engine():
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    return engine


# extract tasks
# extract from alphavantage and load to local postgresql database daily price (in USD) of a given symbol
@task()
def get_daily_price():
    engine = create_sql_engine()
    av_etl.get_daily_price(engine)


# extract from alphavantage and load to local postgresql database daily exchange rate between USD and given currency
@task()
def get_daily_exchange_rate():
    engine = create_sql_engine()
    av_etl.get_daily_exchange_rate(engine)


# transform and load task
# create a new dataframe consisting of close price (in USD) of a stock with a given symbol and close currency exchange
# rate, then calculate the stock price in the choosen currency and add it to the created dataframe and then load
# this dataframe into a database
@task()
def calc_load_daily_price_other_ccy():
    engine = create_sql_engine()
    av_etl.calc_load_daily_price_other_ccy(engine)


# data visualization task
@task()
def visualize_data():
    engine = create_sql_engine()
    av_etl.visualize_data(engine)


# this DAG will be triggered at 00:05 UTC after every business day
with DAG(dag_id="alphavantage_etl_dag",
         schedule_interval="5 0 * * 2-6",
         start_date=datetime(2022, 12, 1, tzinfo=timezone.utc),
         catchup=False,
         tags=["alphavantage"]) as dag:

    # extract
    with TaskGroup("extract_load_src",
                   tooltip="Extract and load the security price in USD and the currency exchange rate") \
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

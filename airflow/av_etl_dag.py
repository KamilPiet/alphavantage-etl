from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from datetime import datetime, timezone
import os
import av_etl
import data_viz
import to_github_pages


def create_sql_engine():
    """Create and then return an SQL engine."""
    conn = BaseHook.get_connection('postgres_alphavantage')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    return engine


# extract tasks
@task()
def get_daily_price():
    engine = create_sql_engine()
    av_etl.get_daily_price(engine)


@task()
def get_daily_exchange_rate():
    engine = create_sql_engine()
    av_etl.get_daily_exchange_rate(engine)


# transform and load task
@task()
def calc_load_daily_price_other_ccy():
    engine = create_sql_engine()
    av_etl.calc_load_daily_price_other_ccy(engine)


# data visualization task
@task()
def visualize_data():
    engine = create_sql_engine()
    report = data_viz.visualize_data(engine)
    working_dir_path = str(os.getenv('AV_ETL_WORKING_DIR_PATH'))
    to_github_pages.publish_report(report, working_dir_path)


# this DAG will be triggered at 12:00 PM UTC every Sunday
with DAG(dag_id="alphavantage_etl_dag",
         schedule_interval="0 12 * * 7",
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

    # define tasks/task groups order
    extract_load_src >> transform_load >> visualize

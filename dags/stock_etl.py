import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from pyspark.sql import SparkSession

from controllers.GlobalUtil import GlobalUtil
from controllers.Extract import Extract
from controllers.Transform import Transform
from controllers.Cleaning import Cleaning
from controllers.Tracker import Tracker

WORKFLOW_DAG_ID = 'stock_etl'
WORKFLOW_START_DATE = datetime.now() - timedelta(days=1)
WORKFLOW_SCHEDULE_INTERVAL = None

WORKFLOW_DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": WORKFLOW_START_DATE,
    "email": ["sb@guided_capstone.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    WORKFLOW_DAG_ID,
    description="ETL for stock data",
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
    catchup=False,
)
GU = GlobalUtil.instance()

APP_NAME = GU.CONFIG['CORE']['APP_NAME']

spark = SparkSession \
    .builder \
    .appName(APP_NAME) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

PROJECT_PATH = GU.CONFIG['CORE']['PROJECT_PATH']
# EXTRACT
extract = Extract(spark)
CSV_FILE = GU.CONFIG['DATA']['SOURCE_DATA_FILE']
CSV_FILE_PATH = os.path.join(GU.PROJECT_PATH, 'data', CSV_FILE)

# CLEAN
clean = Cleaning(spark)
partition_dir = os.path.join(GU.PROJECT_PATH, GU.CONFIG['DATA']['PARTITION_PATH'])
partition_by = GU.CONFIG['DATA']['PARTITION_LABEL']
trade_dir = f'{partition_by}=T'  # 'partition=T'
quote_dir = f'{partition_by}=Q'  # 'partition=Q'

trade_input_dir = os.path.join(partition_dir, trade_dir)
quote_input_dir = os.path.join(partition_dir, quote_dir)

# TRANFORM
trans = Transform(spark)

# Change this value based on the real dataset
date = datetime(2021,1,2)

t1 = PythonOperator(
        task_id='extract_stock',
        python_callable=extract.extract_csv,
        op_kwargs={'filepath': CSV_FILE_PATH,
               },
        dag=dag,
)

t2 = PythonOperator(
        task_id='cleaning_trade',
        python_callable=clean.data_correction_trade,
        op_kwargs={'filepath': trade_input_dir,
               },
        dag=dag,
)

t3 = PythonOperator(
        task_id='cleaning_quote',
        python_callable=clean.data_correction_quote,
        op_kwargs={'filepath': quote_input_dir,
               },
        dag=dag,
)

t4 = PythonOperator(
        task_id='transform',
        python_callable=trans.create_trade_staging_table,
        op_kwargs={'in_date': date,
               },
        dag=dag,
)

t1 >> [t2, t3]
[t2, t3] >> t4
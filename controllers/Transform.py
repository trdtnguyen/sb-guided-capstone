"""
Transform data from cleaned parquet files
Read parquest files (output from Cleaning stage)
Build an ETL job that calculates the following results for a given day
- Latest trade price before the quote
- Latest 30-minutes moving average trade price, before the quote
- The bid/ask price movement from previous day's closing price
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as F
from models.CommonEvent import CommonEvent
from datetime import datetime
import json
import logging
import sys
class Transform:
    def __init__(self):
        app_name = 'Equity Market Data Analysis'
        self.spark = SparkSession \
                .builder \
                .master('local') \
                .appName(app_name) \
                .getOrCreate()
        logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                            format='%(levelname)s - %(message)s')

    def create_trade_staging_table(self, basepath:str, date_str):
        #Read Trade Partition Dataset from the output of the previous stage
        df = self.spark.read.parquet(f'{basepath}/trade/{date_str}')
        df.createOrReplaceTempView("trades")

        # Read trade table with necessary columns and save as a temp view
        df = spark.sql(f"SELECT symbol, exchange, event_time, event_seq_num, trade price
                       FROM trades
                       WHERE trade_dt = {date_str}
                       ")
        df.createOrReplaceTempView("tmp_trade_moving_avg")

        # Read trade table and compute the avg latest 30 minutes
        moving_avg_df = self.spark.sql(f"
                            SELECT symbol, exchange, event_time, event_seq_num, trade_price,
                               AVG(trade_price) OVER (PARTITION BY symbol ORDER BY event_time DESC
                               ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING)  AS running_avg
                            FROM tmp_trade_moving_avg
                            ")
        #Save the temp view into hive table for staging
        moving_avg_df.write.saveAsTable("tmp_trade_moving_avg_table")

        #Get the previous date trade data
        date = datetime.strptime(date_str, '%Y-%m-%d')
        prev_date = date - timedelta(1,0,0) # days, seconds, millisecond
        prev_date_str = prev_date.strftime('%Y-%m-%d')

        # Read Trade partition dataset
        df = self.spark.read.parquet(f'{basepath}/trade/{prev_date_str}')
        df.createOrReplaceTempView("prev_trades")

        df = spark.sql(f"SELECT symbol, exchange, event_time, event_seq_num, trade price
                       FROM prev_trades
                       WHERE trade_dt = {prev_date_str}
                       ")
        # create a temp view
        df.createOrReplaceTempView("tmp_last_trade")

        # Read trade table and compute the avg latest 30 minutes
        last_pr_df = spark.sql(f" SELECT symbol, exchange, last_pr FROM
                               (SELECT symbol, exchange, event_time, event_seq_num, trade_price,
                               AVG(trade_price) OVER (ORDER BY event_time DESC
                               ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING) AS last_pr
                               FROM tmp_last_trade)
                               ")
        last_pr_df.write.saveAsTable("tmp_last_trade_table")


        # Read Quote partition dataset
        # Remind that columns in quotes table are: arrival_time, trade_dt, symbol, exchange, event_time, event_seq_num, bid_price, bid_size, ask_price, ask_size
        df = self.spark.read.parquet(f'{basepath}/quote/{date_str}')
        df.createOrReplaceTempView("quotes")








t = Transform()

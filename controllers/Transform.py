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

from controllers.GlobalUtil import  GlobalUtil
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from datetime import datetime, timedelta
import logging
import os
import sys
import configparser

from controllers.Tracker import Tracker


class Transform:
    def __init__(self, spark: SparkSession):
        self.GU = GlobalUtil.instance()
        self.spark = spark


    def create_trade_staging_table(self, in_date):
        tracker = Tracker(self.GU.CONFIG['CORE']['JOB_NAME_TRANSFORM'])

        try:
            date_str = in_date.strftime('%Y-%m-%d')
            #############
            ##### Step 1. Read trade and quote data from daily_data for date_str
            #############
            daily_data_dir = os.path.join(self.PROJECT_PATH, self.CONFIG['DATA']['DAILY_DATA_PATH'])
            prefix = self.CONFIG['DATA']['DAILY_DATA_PREFIX']
            input_trade_sub_dir = 'trade'
            input_quote_sub_dir = 'quote'
            input_trade_dir = os.path.join(daily_data_dir, input_trade_sub_dir, f'{prefix}={date_str}')
            input_quote_dir = os.path.join(daily_data_dir, input_quote_sub_dir, f'{prefix}={date_str}')

            #Read Trade Partition Dataset from the output of the previous stage
            trade_df = self.spark.read.parquet(input_trade_dir)
            quote_df = self.spark.read.parquet(input_quote_dir)

            # sql_text = "SELECT symbol, exchange, event_time, event_seq_num, trade_price " + \
            #            "FROM trades " + \
            #            f"WHERE trade_dt = {date_str})"
            # Reorder columns and filter out the date
            trade_df.select(trade_df['trade_dt'], trade_df['symbol'], trade_df['exchange'], trade_df['event_time'], trade_df['event_seq_num'],
                      trade_df['trade_price']).filter(trade_df['trade_dt'] == in_date)
            # trade_df.show()


            trade_df.createOrReplaceTempView("trades")
            quote_df.createOrReplaceTempView('quotes')

            #################
            ##### Step 2. Compute average 30 minutes moving window trade price
            #################
            sql_text = "SELECT trade_dt, symbol, exchange, event_time, event_seq_num, trade_price, " + \
                       "AVG(trade_price) OVER (PARTITION BY symbol ORDER BY event_time " + \
                       "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_trade_price " + \
                       f"FROM trades;"
            moving_avg_df = self.spark.sql(sql_text)
            print('moving_avg_df: ==================')
            # moving_avg_df.show()

            #################
            ##### Step 3. Compute last prices from trade_moving_avg_table
            #################
            #Save the temp view into hive table for staging
            # moving_avg_df.write.saveAsTable("trade_moving_avg_table")
            moving_avg_df.createOrReplaceTempView("trade_moving_avg_table")

            sql_text = "SELECT symbol, exchange, event_time, event_seq_num, trade_price, " + \
                "LAST_VALUE(trade_price) " +\
                "OVER (PARTITION BY symbol ORDER BY event_time " + \
                       "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_trade_price, " +\
                "LAST_VALUE(avg_trade_price) " +\
                "OVER (PARTITION BY symbol ORDER BY event_time " +\
                        "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_avg_trade_price " +\
                "FROM trade_moving_avg_table"
            last_moving_avg_df = self.spark.sql(sql_text)
            # last_moving_avg_df.write.saveAsTable("last_trade_moving_avg_table")
            last_moving_avg_df.createOrReplaceTempView("last_trade_moving_avg_table")
            print('last_moving_avg_df: ==================')
            last_moving_avg_df.show()
            #################
            ##### Step 4. Join last_moving_avg_df and quote
            #################
            print('====== quotes: ')
            quote_df.show()
            sql_text = "SELECT DISTINCT q.trade_dt, 'Q', q.symbol, q.event_time, q.event_seq_num, " +\
                "q.exchange, q.bid_price, q.bid_size, q.ask_price, q.ask_size, " +\
                "t. last_trade_price, t.last_avg_trade_price " +\
                "FROM quotes as q, last_trade_moving_avg_table as t " +\
                "WHERE q.symbol = t.symbol"
                # "WHERE q.symbol = t.symbol and q.event_time = t.event_time"
            join_df = self.spark.sql(sql_text)
            print('join_df: ==================')
            join_df.show()
            join_df.createOrReplaceTempView('join_tb')

            #############
            ##### Step 5. Get the previous date trade data and comptute the close price as
            #### the last average moving trade price
            #############
            prev_date = in_date - timedelta(1,0,0) # days, seconds, millisecond
            prev_date_str = prev_date.strftime('%Y-%m-%d')


            input_prev_trade_dir = os.path.join(daily_data_dir, input_trade_sub_dir, f'{prefix}={prev_date_str}')


            # Read Trade Partition Dataset from the output of the previous stage
            prev_trade_df = self.spark.read.parquet(input_prev_trade_dir)
            prev_trade_df.select(
                prev_trade_df['trade_dt'], prev_trade_df['symbol'],
                prev_trade_df['exchange'], prev_trade_df['event_time'],
                prev_trade_df['event_seq_num'], prev_trade_df['trade_price'])\
                .filter(prev_trade_df['trade_dt'] == prev_date)
            # trade_df.show()
            prev_trade_df.createOrReplaceTempView("prev_trades")

            ###Compute average 30 minutes moving window trade price
            sql_text = "SELECT trade_dt, symbol, exchange, event_time, event_seq_num, trade_price, " + \
                       "AVG(trade_price) OVER (PARTITION BY symbol ORDER BY event_time " + \
                       "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_trade_price " + \
                       f"FROM prev_trades;"
            prev_moving_avg_df = self.spark.sql(sql_text)

            ##Compute last prices from trade_moving_avg_table

            #Save the temp view into hive table for staging
            prev_moving_avg_df.createOrReplaceTempView("prev_trade_moving_avg_table")

            sql_text = "SELECT DISTINCT symbol, exchange, " + \
                "LAST_VALUE(avg_trade_price) " +\
                "OVER (PARTITION BY symbol ORDER BY event_time " +\
                        "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price " +\
                "FROM prev_trade_moving_avg_table"
            prev_last_moving_avg_df = self.spark.sql(sql_text)
            # last_moving_avg_df.write.saveAsTable("last_trade_moving_avg_table")
            prev_last_moving_avg_df.createOrReplaceTempView("prev_last_trade_moving_avg_table")

            # Nornal join use spark.sql
            # sql_text = "SELECT q.trade_dt, q.symbol, event_time, event_seq_num, q.exchange, " +\
            #     "bid_price, bid_size, ask_price, ask_size, " +\
            #     "last_trade_price, last_avg_trade_price, " +\
            #     "bid_price - close_price as bid_pr_mv, " +\
            #     "ask_price - close_price as ask_pr_mv " +\
            #     "FROM join_tb as q, prev_last_trade_moving_avg_table as t " +\
            #     "WHERE q.symbol = t.symbol and q.exchange = t.exchange"
            # final_df = self.spark.sql(sql_text)

            # broadcast join
            prev_last_moving_avg_df.show()
            join_cond = [join_df['symbol'] == prev_last_moving_avg_df['symbol'],
                         join_df['exchange'] == prev_last_moving_avg_df['exchange']
                         ]
            final_df = join_df.join(prev_last_moving_avg_df,
                                    (join_df['symbol'] == prev_last_moving_avg_df['symbol']) &
                                    (join_df['exchange'] == prev_last_moving_avg_df['exchange'])
                                    )


            # final_df = join_df.join(prev_last_moving_avg_df,
            #                         (join_df['symbol'] == prev_last_moving_avg_df['symbol']) &
            #                         (join_df['exchange'] == prev_last_moving_avg_df['exchange'])
            #                         )\
            #     .select(join_df['trade_dt'], join_df['symbol'], join_df['event_time'],
            #             join_df['event_seq_num'], join_df['exchange'],
            #             join_df['bid_price'], join_df['bid_size'], join_df['ask_price'], join_df['ask_size'] ,
            #             join_df['last_trade_price'], join_df['last_avg_trade_price'],
            #             (join_df['bid_price'] - prev_last_moving_avg_df['close_price']).alias('bid_pr_mv'),
            #             (join_df['ask_price'] - prev_last_moving_avg_df['close_price']).alias('ask_pr_mv'),
            #             ).distinct()
            final_df.show()


            #############
            ##### Step 6. Write on the database
            #############
            analytic_data_dir = os.path.join(self.PROJECT_PATH, self.CONFIG['DATA']['ANALYTIC_DATA_PATH'])
            if not os.path.exists(analytic_data_dir):
                os.makedirs(analytic_data_dir)

            prefix = self.CONFIG['DATA']['ANALYTIC_DATA_PREFIX']
            output_dir = os.path.join(analytic_data_dir, f'{prefix}={date_str}')
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            print(f'Write data on {analytic_data_dir} ...', end = ' ')
            final_df.write.mode('append').parquet(output_dir)
            print('Done.')

            tracker.update_job_status("success")
        except Exception as e:
            logging.error('Error on extract_csv')
            print(e)

            tracker.update_job_status("failed")


### Self test
GU = GlobalUtil.instance()

app_name = GU.CONFIG['CORE']['APP_NAME']
spark = SparkSession \
    .builder \
    .master('local') \
    .appName(app_name) \
    .getOrCreate()

t = Transform(spark)
date = datetime(2021,1,2)
t.create_trade_staging_table(date)


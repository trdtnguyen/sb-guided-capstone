"""
Cleaning data from temp parquet file
Read parquet file from temp location
Select necessary columns
Keep the latest version data and discard the older ones
Write to parquest files partition by date
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
class Cleaning:
    def __init__(self):
        app_name = 'Equity Market Data Analysis'
        self.spark = SparkSession \
                .builder \
                .master('local') \
                .appName(app_name) \
                .getOrCreate()
        logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                            format='%(levelname)s - %(message)s')

    def data_correction_trade(self, filepath:str):
        #Read Trade Partition Dataset from temporary location
        trade_common = self.spark.read.parquet(filepath)

        # select necessary of trade records.
        trade_df = trade_common.select("arrival_time", "trade_dt", "symbol", "exchange", "event_time", "event_seq_num", "trade_price", "trade_size")
        trade_df.show()

        # composite key: trade_dt, symbol, exchange, event_time, event_seq_num
        #trade_grouped_df = trade_df.groupBy("trade_dt", "symbol", "exchange", "event_time", "event_seq_num")
        trade_grouped_df = trade_df.orderBy("arrival_time") \
        .groupBy("trade_dt", "symbol", "exchange", "event_time", "event_seq_num") \
        .agg(F.collect_set("arrival_time").alias("arrival_times"), F.collect_set("trade_price").alias("trade_prices"), F.collect_set("trade_size").alias("trade_sizes"))
        trade_grouped_df.show(truncate=False)

        trade_removed_dup_df = trade_grouped_df \
        .withColumn("arrival_time", F.slice(trade_grouped_df["arrival_times"], 1, 1)[0]) \
        .withColumn("trade_price", F.slice(trade_grouped_df["trade_prices"], 1, 1)[0]) \
        .withColumn("trade_size", F.slice(trade_grouped_df["trade_sizes"], 1, 1)[0])

        trade_final_df = trade_removed_dup_df \
        .drop(F.col("arrival_times")) \
        .drop(F.col("trade_prices")) \
        .drop(F.col("trade_sizes"))

        trade_final_df.show(truncate=False)
        #Write the remain data to parquet files
        trade_date = datetime.today()
        trade_date_str = trade_date.strftime('%Y-%m-%d')
        trade_final_df.write.mode('append').parquet(f'trade/trade-dt={trade_date_str}')


    def data_correction_quote(self, filepath:str):
        #Read Trade Partition Dataset from temporary location
        quote_common = self.spark.read.parquet(filepath)

        # select necessary of trade records.
        quote_df = quote_common.select("arrival_time", "trade_dt", "symbol", "exchange", "event_time", "event_seq_num", "bid_price", "bid_size", "ask_price", "ask_size")
        quote_df.show()

        # composite key: trade_dt, symbol, exchange, event_time, event_seq_num
        quote_grouped_df = quote_df.orderBy("arrival_time") \
        .groupBy("trade_dt", "symbol", "exchange", "event_time", "event_seq_num") \
        .agg(F.collect_set("arrival_time").alias("arrival_times"), \
             F.collect_set("bid_price").alias("bid_prices"), \
             F.collect_set("bid_size").alias("bid_sizes"), \
             F.collect_set("ask_price").alias("ask_prices"), \
             F.collect_set("ask_size").alias("ask_sizes"))
        quote_grouped_df.show(truncate=False)

        quote_removed_dup_df = quote_grouped_df \
        .withColumn("arrival_time", F.slice(quote_grouped_df["arrival_times"], 1, 1)[0]) \
        .withColumn("bid_price", F.slice(quote_grouped_df["bid_prices"], 1, 1)[0]) \
        .withColumn("bid_size", F.slice(quote_grouped_df["bid_sizes"], 1, 1)[0]) \
        .withColumn("ask_price", F.slice(quote_grouped_df["ask_prices"], 1, 1)[0]) \
        .withColumn("ask_size", F.slice(quote_grouped_df["ask_sizes"], 1, 1)[0])

        quote_final_df = quote_removed_dup_df \
        .drop(F.col("arrival_times")) \
        .drop(F.col("bid_prices")) \
        .drop(F.col("bid_sizes")) \
        .drop(F.col("ask_prices")) \
        .drop(F.col("ask_sizes"))

        quote_final_df.show(truncate=False)
        #Write the remain data to parquet files
        trade_date = datetime.today()
        trade_date_str = trade_date.strftime('%Y-%m-%d')
        quote_final_df.write.mode('append').parquet(f'quote/trade-dt={trade_date_str}')




c = Cleaning()
c.data_correction_trade('output_dir/partition=T')
c.data_correction_quote('output_dir/partition=Q')

"""
Transform data from temp parquet file
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

        #trade_grouped_df = trade_grouped_df.orderBy("arrival_time")
        #trade_grouped_df.show()

        #Remove rows with old data


        #Write the remain data to parquet files

t = Transform()
t.data_correction_trade('output_dir/partition=T')

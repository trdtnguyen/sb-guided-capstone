"""
Transform data from temp parquet file
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

from pyspark.sql import SparkSession
from pyspark import SparkContext
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
        trade_df = trade.common.select("arrival_time", "trade_dt", "symbol", "exchange", "event_time", "event_seq_num", "trade_price", "trade_size")
        trade_df.show()

        # composite key: trade_dt, symbol, exchange, event_time, event_seq_num
        trade_grouped_df = trade_df.groupBy("trade_dt", "symbol", "exchange", "event_time", "event_seq_num").orderBy("arrival_time")
        trade_grouped_df.show()

        #Remove rows with old data


        #Write the remain data to parquet files

t = Transform()
t.data_correction_trade('output_dir/partition=T')

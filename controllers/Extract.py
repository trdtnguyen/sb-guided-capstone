"""
Extract data sources
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

from pyspark.sql import SparkSession
from pyspark import SparkContext
from models.CommonEvent import CommonEvent
from datetime import datetime
import json
"""
parse the CSV line to CommonEvent object
format of CSV line:
    Trade:
    TradeDate,RecordType,Symbol,ExecutionID,EventTime, Event Sequence Number,Exchange,Trade Price, Trade Size

    Quote:
    TradeDate, RecordType, Symbol,EventTime, Event Sequence Number,Exchange, BidPrice, BidSize, AskPrice, AskSize
"""


def parse_csv(line: str) -> CommonEvent:
    record_type_pos = 1
    record = line.split(',')
    try:

        if record[record_type_pos] == 'T':
            # TradeDate,RecordType,Symbol,ExecutionID,EventTime, Event Sequence Number,Exchange,Trade Price, Trade Size
            partition = 'T'
            trade_dt = datetime.strptime(record[0], '%Y-%m-%d')
            rec_type = record[1]
            symbol = record[2]
            event_time = datetime.strptime(record[4], '%Y-%m-%d')
            event_seq_num = int(record[5])
            exchange = record[6]
            arrival_time = datetime.now()
            trade_price = float(record[7])
            trade_size = int(record[8])
            bid_price = 0.0
            bid_size = 0
            ask_price = 0.0
            ask_size = 0
            line = ''

            event = CommonEvent(partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                                arrival_time,
                                trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                                line)
            return event
        elif record[record_type_pos] == 'Q':
            # TradeDate, RecordType, Symbol,EventTime, Event Sequence Number,Exchange, BidPrice, BidSize, AskPrice,
            # AskSize
            partition = 'Q'
            trade_dt = datetime.strptime(record[0], '%Y-%m-%d')
            rec_type = record[1]
            symbol = record[2]
            event_time = datetime.strptime(record[3], '%Y-%m-%d')
            event_seq_num = int(record[4])
            exchange = record[5]
            arrival_time = datetime.now()
            trade_price = 0.0
            trade_size = 0
            bid_price = float(record[6])
            bid_size = int(record[7])
            ask_price = float(record[8])
            ask_size = int(record[9])
            line = ''

            event = CommonEvent(partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                                arrival_time,
                                trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                                line)
            return event
        else:
            raise Exception
    except Exception as e:
        event = CommonEvent('B', trade_dt=None, rec_type=None, symbol=None, event_time=None, event_seq_num=None, exchange=None,
                                arrival_time=None,
                                trade_price=None, trade_size=None, bid_price=None, bid_size=None, ask_price=None, ask_size=None,line=line)
        return event


"""
parse the JSON line to CommonEvent object
format of JSON line:
    Trade:
    TradeDate,RecordType,Symbol,ExecutionID,EventTime, Event Sequence Number,Exchange,Trade Price, Trade Size

    Quote:
    TradeDate, RecordType, Symbol,EventTime, Event Sequence Number,Exchange, BidPrice, BidSize, AskPrice, AskSize
"""


def parse_json(line: str) -> CommonEvent:
    record_type_pos = 1
    record = json.loads(line)

    try:

        if record['record_type'] == 'T':
            # TradeDate,RecordType,Symbol,ExecutionID,EventTime, Event Sequence Number,Exchange,Trade Price, Trade Size
            name_index = ['trade_date', 'record_type', 'symbol', 'executionid', "event_time",
                          'event_seq_num', 'exchange', 'trade_price', 'trade_size']
            partition = 'T'
            trade_dt = datetime.strptime(record[name_index[0]], '%Y-%m-%d')
            rec_type = record[name_index[1]]
            symbol = record[name_index[2]]
            event_time = datetime.strptime(record[name_index[4]], '%Y-%m-%d')
            event_seq_num = int(record[name_index[5]])
            exchange = record[name_index[6]]
            arrival_time = datetime.now()
            trade_price = float(record[name_index[7]])
            trade_size = int(record[name_index[8]])
            bid_price = 0.0
            bid_size = 0
            ask_price = 0.0
            ask_size = 0
            line = ''

            event = CommonEvent(partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                                arrival_time,
                                trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                                line)
            return event
        elif record[record_type_pos] == 'Q':
            # TradeDate, RecordType, Symbol,EventTime, Event Sequence Number,Exchange, BidPrice, BidSize, AskPrice,
            # AskSize
            name_index = ['trade_date', 'record_type', 'symbol', "event_time",
                          'event_seq_num', 'exchange', 'bid_price', 'bid_size', 'ask_price', 'ask_size']
            partition = 'Q'
            trade_dt = datetime.strptime(record[name_index[0]], '%Y-%m-%d')
            rec_type = record[name_index[1]]
            symbol = record[name_index[2]]
            event_time = datetime.strptime(record[name_index[3]], '%Y-%m-%d')
            event_seq_num = int(record[name_index[4]])
            exchange = record[name_index[5]]
            arrival_time = datetime.now()
            trade_price = 0.0
            trade_size = 0
            bid_price = float(record[name_index[6]])
            bid_size = int(record[name_index[7]])
            ask_price = float(record[name_index[8]])
            ask_size = int(record[name_index[9]])
            line = ''

            event = CommonEvent(partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                                arrival_time,
                                trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                                line)
            return event
        else:
            raise Exception
    except Exception as e:
        event = CommonEvent()
        event.partition = 'B'
        event.original_line = line
        return event


class Extract:
    def __init__(self):
        app_name = 'Equity Market Data Analysis'
        self.spark = SparkSession \
            .builder \
            .master('local') \
            .appName(app_name) \
            .getOrCreate()

    """Extract data from csv data source
    filepath: path to the data file. If the file is on Azure store, the file path is:
    wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/<path_in_container>"
    parse_f: function for parse data. Must be 'parse_csv' or 'parse_json'
    config_key: used when a file is on the cloud. If the file is on Azure store, the value is
    "fs.azure.account.key.<storage-account-name>.blob.core.windows.net"
    config_value: used when a file is on the cloud. If the file is on Azure store, the value is your account access key
    """
    def extract_csv(self, filepath:str, config_key:str, config_value:str):
        self.spark.conf.set(config_key, config_value)
        raw = self.spark.sparkContext.textFile(filepath)
        #raw = self.spark.read.csv(filepath, comment='#')
        parsed = raw.map(lambda line: parse_csv(line))
        data = self.spark.createDataFrame(parsed)
        data.write.partitionBy('partition').mode('overwrite').parquet('output_dir')

    """Extract data from data source
    filepath: path to the data file. If the file is on Azure store, the file path is:
    wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/<path_in_container>"
    parse_f: function for parse data. Must be 'parse_csv' or 'parse_json'
    config_key: used when a file is on the cloud. If the file is on Azure store, the value is
    "fs.azure.account.key.<storage-account-name>.blob.core.windows.net"
    config_value: used when a file is on the cloud. If the file is on Azure store, the value is your account access key
    """
    def extract_json(self, filepath:str, config_key:str, config_value:str):
        self.spark.conf.set(config_key, config_value)
        raw = self.spark.read.json(filepath)
        parsed = raw.map(lambda line: parse_json(line))
        data = self.spark.createDataFrame(parsed)
        data.write.partitionBy('partition').mode('overwrite').parquet('output_dir')

e = Extract()
e.extract_csv('walmart_stock.csv', 'key','value')

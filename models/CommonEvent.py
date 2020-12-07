"""
Data Model for CommonEvent
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

from datetime import datetime


class CommonEvent:
    def __init__(self):
        self.trade_dt = datetime.now()
        self.rec_type = 'T'  # 'T' for Trade or 'Q' for Quote
        self.symbol = ''
        self.event_time = datetime.now()  # Event time
        self.exchange = ''
        self.event_seq_num = 0  # Event sequence number
        self.arrival_time = datetime.now()  # Arrival time
        self.trade_price = 0.0  # Trade price
        self.trade_size = 0
        self.bid_price = 0.0
        self.bid_size = 0
        self.ask_price = 0.0
        self.ask_size = 0
        self.partition = 'T'  # 'T' for Trade , 'Q' for Quote, or 'B' for bad record
        self.original_line = ''  # used for bad record, keep the original record for checking

    def __init__(self, partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange, arrival_time,
                 trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                 line):
        self.partition = partition
        self.trade_dt = trade_dt
        self.rec_type = rec_type
        self.symbol = symbol
        self.event_time = event_time
        self.event_seq_num = event_seq_num
        self.exchange = exchange
        self.arrival_time = arrival_time
        self.trade_price = trade_price
        self.trade_size = trade_size
        self.bid_price = bid_price
        self.bid_size = bid_size
        self.ask_price = ask_price
        self.ask_size = ask_size
        self.original_line = line
    def __str__(self):
        result = ''
        if self.rec_type in ['T', 'Q']:
            result += f"({self.trade_dt.strftime('%Y-%m-%d')}, {self.rec_type}, {self.symbol}, {self.event_time.strftime('%Y-%m-%d')}, \
                        {self.exchange}, {self.event_seq_num}, {self.arrival_time.strftime('%Y-%m-%d')}, {self.trade_price}, \
                        {self.trade_size}, {self.bid_price}, {self.bid_size}, {self.ask_price}, {self.ask_size} "
        else:
            result += self.original_line
        return result

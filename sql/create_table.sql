CREATE DATABASE stock_trade;
use stock_trade;

CREATE TABLE IF NOT EXISTS trade_raw(
	trade_date datetime not null,
    record_type varchar(1), -- T for trade, Q for quote
    symbol varchar(64),
    execution_id varchar(64),
    event_time timestamp,
    event_seq_number int,
    exchange varchar(64),
    trade_price decimal,
    trade_size int
);

CREATE TABLE IF NOT EXISTS quote_raw(
	trade_date datetime not null,
    record_type varchar(1), -- T for trade, Q for quote
    symbol varchar(64),
    event_time timestamp,
    event_seq_number int,
    exchange varchar(64),
    bid_price decimal,
    bid_size int,
    ask_price decimal,
    ask_size int
);

# Equity Market Data Analysis
This project implement a basic ETL pipeline used to acquire daily trade prices and quotes from stock market and analytic them based on the difference of the close prices of the previous day and the current day. 

The project exploits Spark for extracting data source, cleaning, and transforming raw data to meaningful data that ready for analytic.

This is a guided project from Springboard's Data Engineering career track.



## Project Setup
### Requirements

***Standalone project without container:***
* postgresql installed
* python packages in requirements.txt
* Spark installed and properly configued

***Running on docker container***
* docker and docker-compose installed

### Database setup
* Create database
```
$ createdb -h localhost -p 5432 -U postgres sbstock
```
* Create table
```
$ psql -h localhost -U postgres -d sbstock -f sql/create_table.sql
```

* JDBC connector setup
This project has already provided the jar file for JDBC connector for Postgresql. In case you want to download a newer version than the provided one, follow step 1. Otherwise, skip step 1.
* Step 1: Download JDBC connector for postgresql
```
$ cd jars
$ wget https://jdbc.postgresql.org/download/postgresql-42.2.18.jar
```
* Step 2: Copy JDBC jar file to your spark jars
```
$ cp postgresql-42.2.18.jar $SPARK_HOME/jars/
```

## Data Model
### Trade data (source)
Column | Type
-------|-----
Trade Date| Date
Record Type |Varchar(1)
Symbol |String
***Execution ID*** |String
Event Time |Timestamp
Event Sequence Number | Int
Exchange | String
***Trade Price*** | Decimal
***Trade Size*** | Int

### Quote data (source)
Column | Type
-------|-----
Trade Date| Date
Record Type |Varchar(1)
Symbol |String
Event Time |Timestamp
Event Sequence Number | Int
Exchange | String
***Bid Price*** | Decimal
***Bid Size*** | Int
***Ask Price*** | Decimal
***Ask Size*** | Int

***Note:***
* The ***bid price*** refers to the highest price a buyer will pay for a security.
* The ***ask price*** refers to the lowest price a seller will accept for a security.

For example, company A wants to purchase 1,000 shares of XYZ stock at $10, and company B wants to sell 1,500 shares of XYZ at $10.25. The spread is the difference between the asking price of $10.25 and the bid price of $10, or 25 cents. An individual investor looking at this spread would then know that, if they want to sell 1,000 shares, they could do so at $10 by selling to A. Conversely, the same investor would know that they could purchase 1,500 shares from B at $10.25. 


### Common Event (dimentional)
Data models in Trade and Quote share some common columns. We combine them into one data model called Common Event as below:
Column | Type | Note
-------|-----|-------
trade_dt| Date | Trade Date (common)
rec_type | String | Record Type, Trade is 'T', Quote is 'Q' (common)
symbol | String | Symbol (common)
exchange | String | Exchange name (common)
event_tm | Timestamp | Event time (common)
event_seq_nb | Integer | Event sequence number (common)
arrival_tm | Timestamp | Arrival time, get in digestion stage
trade_price | Decimal | Trade Price (get from Trade)
trade_size | Integer | Trase size (get from Trade)
bid_price | Decimal | Bid Price (get from Quote)
bid_size | Integer | Bid Size (get from Quote)
ask_price | Decimal | Ask Price (get from Quote)
ask_size | Integer | Ask Price (get from Quote)
partition | String | 'T' for Trade records, 'Q' for quote records, 'B' for bad format record

## Data Sources
This project extract data from stock trades and stock quotes data source on MS Azure with two format CSV and JSON. A sample CSV file would look like:
```
 #TradeDate,RecordType,Symbol,ExecutionID,EventTime,EventSeqNum,Exchange,TradePrice,TradeSize
 2020-01-01,T,Symbol1,1,2020-11-23,1,Exchange1, 59.970001,12
 2020-01-01,T,Symbol2,2,2020-11-23,2,Exchange1, 134.213,120
 2020-01-01,T,Symbol3,3,2020-11-23,3,Exchange2, 10.4712,1301
 2020-01-01,Q,Symbol1,2020-11-23,4,Exchange1, 61,1301, 20, 58.231, 17
 2020-01-01,Q,Symbol2,2020-11-23,4,Exchange1, 61,1301, 20, 58.231, 17
 2020-01-01,Q,Symbol2,2020-11-23,4,Exchange1, 61,1301, 20,
```
***Notes***
* Different record types (`T` for trade and `Q` for quote) have different line format.
* Trade row and quote row must share comon values on `exchange` column. In other words, intersect between `exchange` values in trade rows and `exchange` values in quote rows must be a non-empty set.


## Digestion Process
This is the firs step in the project to extract data from datasources, cleaning and transform the data to parquet files using Apache Spark. The details are:
* Although trade data and quote data have different format, they share common columns such as `TradeDate`, `RecordType`, `Symbol`, etc. So we create a dimentional table `CommonEvent` to combine both data type into one table.
* Implement `CommonEvent` class in Python that represent the `CommonEvent` table. We use this module to capture the extracted data during digestion process.
* Implement `Extract` module to extract data from CSV, JSON format to parquet files and locate those files in partition in HDFS.

The simple code for digesting CSV file is:
```
 def extract_csv(self, filepath:str, config_key:str, config_value:str):
         self.spark.conf.set(config_key, config_value)
         raw = self.spark.sparkContext.textFile(filepath)
         #raw = self.spark.read.csv(filepath, comment='#')
         parsed = raw.map(lambda line: parse_csv(line))
         data = self.spark.createDataFrame(parsed)
         data.write.partitionBy('partition').mode('overwrite').parquet('output_dir')
```
and extract json file:

```
 def extract_json(self, filepath:str, config_key:str, config_value:str):
         self.spark.conf.set(config_key, config_value)
         raw = self.spark.read.json(filepath)
         parsed = raw.map(lambda line: parse_json(line))
         data = self.spark.createDataFrame(parsed)
         data.write.partitionBy('partition').mode('overwrite').parquet('output_dir')
```

To test the digestion process run:
```
$ python controllers/Extract.py
```

As the results, parquet files are created in output directory as:
```
output_dir/
├── partition=B
│   └── part-00000-356a74e9-e5e0-4a44-9e37-ea25f28af432.c000.snappy.parquet
├── partition=Q
│   └── part-00000-356a74e9-e5e0-4a44-9e37-ea25f28af432.c000.snappy.parquet
├── partition=T
│   └── part-00000-356a74e9-e5e0-4a44-9e37-ea25f28af432.c000.snappy.parquet
└── _SUCCESS

```
### Developer Note:
* In `extract_csv()` funciton, we use `.map()` funciton of `RDD` with `lambda` function syntax that apply `parse_csv(line)` for each item in the `raw` RDD. The return type of `parse_csv()` is `CommonEvent` object. The `parsed` variable has type as `PipelineRDD`.
* We then convert `PipelineRDD` to `DataFrame` using `spark.createDataFrame(parsed)`. The output of `data.printSchema()` would be:

```
root
 |-- arrival_time: timestamp (nullable = true)
 |-- ask_price: double (nullable = true)
 |-- ask_size: long (nullable = true)
 |-- bid_price: double (nullable = true)
 |-- bid_size: long (nullable = true)
 |-- event_seq_num: long (nullable = true)
 |-- event_time: timestamp (nullable = true)
 |-- exchange: string (nullable = true)
 |-- original_line: string (nullable = true)
 |-- partition: string (nullable = true)
 |-- rec_type: string (nullable = true)
 |-- symbol: string (nullable = true)
 |-- trade_dt: timestamp (nullable = true)
 |-- trade_price: double (nullable = true)
 |-- trade_size: long (nullable = true)

```

* The output of `data.show()` would be:
```
+--------------------+---------+--------+---------+--------+-------------+-------------------+---------+--------------------+---------+--------+-------+-------------------+-----------+----------+
|        arrival_time|ask_price|ask_size|bid_price|bid_size|event_seq_num|         event_time| exchange|       original_line|partition|rec_type| symbol|           trade_dt|trade_price|trade_size|
+--------------------+---------+--------+---------+--------+-------------+-------------------+---------+--------------------+---------+--------+-------+-------------------+-----------+----------+
|                null|     null|    null|     null|    null|         null|               null|     null|#TradeDate,Record...|        B|    null|   null|               null|       null|      null|
|2020-12-07 13:10:...|      0.0|       0|      0.0|       0|            1|2020-11-23 00:00:00|Exchange1|                    |        T|       T|Symbol1|2020-01-01 00:00:00|  59.970001|        12|
|2020-12-07 13:10:...|      0.0|       0|      0.0|       0|            2|2020-11-23 00:00:00|Exchange1|                    |        T|       T|Symbol2|2020-01-01 00:00:00|    134.213|       120|
|2020-12-07 13:10:...|      0.0|       0|      0.0|       0|            3|2020-11-23 00:00:00|Exchange2|                    |        T|       T|Symbol3|2020-01-01 00:00:00|    10.4712|      1301|
|2020-12-07 13:10:...|   58.231|      17|  61.1301|      20|            4|2020-11-23 00:00:00|Exchange1|                    |        Q|       Q|Symbol1|2020-01-01 00:00:00|        0.0|         0|
|2020-12-07 13:10:...|   58.231|      17|  61.1301|      20|            4|2020-11-23 00:00:00|Exchange1|                    |        Q|       Q|Symbol2|2020-01-01 00:00:00|        0.0|         0|
|                null|     null|    null|     null|    null|         null|               null|     null|2020-01-01,Q,Symb...|        B|    null|   null|               null|       null|      null|
+--------------------+---------+--------+---------+--------+-------------+-------------------+---------+--------------------+---------+--------+-------+-------------------+-----------+----------+

```
One interesting observation is that when using `map()` with the function paramenter return the object, Spark RDD automatically creates the header based on attribute names in our `CommonEvent` class in ascending alphabet order.

## End-of-day Data Load (Cleaning)
In the previous stage, digestion process keep update in-day data into parquet files located in temporary locations i.e., `output_dir`. This stage load and clean data as following steps:
* Read parquet files from temporary location
* Select the necessary columns for `trade` and `quote` records
* Apply data correction i.e., keep only the latest price and remove the older one.
* Write the dataset back to parquet files on Azure Blob Storage

* ***Input***: parquet files in `output_dir` partitioned by type (`T`, `Q`, 'B`)
* ***Output***: parquet files in `trade` or `quote` directory for each day (e.g., `trade/trade-dt=2020-12-20`)

### Why applying data correction is needed?
Trade and quote data are updated periodically in a day by Exchange. For each row, the composite key is `trade_dt`, `symbol`, `exchange`, `event_tm`,  `event_sq_num`. During the day, the Exchange could update data with the same composite key to correct the previous one. So in our digested data, there are rows with the same composite key but different prices.

### How to handle duplicate composite key records?
* Choose `arrival_time` as the ***unique key*** in `CommonEvent` table.
```python
#Read Trade Partition Dataset from temporary location
trade_common = self.spark.read.parquet(filepath)
# select necessary of trade records.
trade_df = trade_common.select("arrival_time", "trade_dt", "symbol", "exchange", "event_time", "event_seq_num", "trade_price", "trade_size")
```
* Sort the records by `arrivel_time` and aggregate duplicated composite key rows into a group using `pyspark.sql.functions.collect_set()`
```python
# composite key: trade_dt, symbol, exchange, event_time, event_seq_num
trade_grouped_df = trade_df.orderBy("arrival_time") \
         .groupBy("trade_dt", "symbol", "exchange", "event_time", "event_seq_num") \
         .agg(F.collect_set("arrival_time").alias("arrival_times"), F.collect_set("trade_price").alias("trade_prices"), F.collect_set("trade_size").alias("trade_sizes")
```

* Make a new column by selecting the first item in each aggregated group in the previous step. This column is the latest price we need to keep, the remains are discard.

```python
trade_removed_dup_df = trade_grouped_df \
         .withColumn("arrival_time", F.slice(trade_grouped_df["arrival_times"], 1, 1)[0]) \
         .withColumn("trade_price", F.slice(trade_grouped_df["trade_prices"], 1, 1)[0]) \
         .withColumn("trade_size", F.slice(trade_grouped_df["trade_sizes"], 1, 1)[0])
```
* Discard unnessary columns
```python
trade_final_df = trade_removed_dup_df \
         .drop(F.col("arrival_times")) \
         .drop(F.col("trade_prices")) \
         .drop(F.col("trade_sizes"))
```
## Analytical ETL
In the previous stage, cleaning process removed unnecessary data, corrupted data and older data in a daily basic and save data in `trade` and `quote` directory in Azure Blob Storage. This stage loads data from the previous step into Spark and transform the data as following steps:

* Step 1: Load ***trade*** data for the current day into a temp view `v1`.
An sample output of `v1`:

```
arrival_time,trade_dt,symbol,exchange,event_time,event_seq_num,trade_price,trade_size
"2020-12-20 12:00:00","2020-12-20 00:00:00",ABC,Exchange1,"2020-12-20 10:00:00",1,20.12,120
"2020-12-20 13:00:00","2020-12-20 00:00:00",ABC,Exchange1,"2020-12-20 10:10:00",2,22.32,130
"2020-12-20 14:00:00","2020-12-20 00:00:00",ABC,Exchange1,"2020-12-20 10:20:00",3,21.2,125
"2020-12-20 15:00:00","2020-12-20 00:00:00",ABC,Exchange1,"2020-12-20 10:30:00",4,25.12,100
"2020-12-20 12:00:00","2020-12-20 00:00:00",DEF,Exchange1,"2020-12-20 10:00:00",1,20.12,120
"2020-12-20 13:00:00","2020-12-20 00:00:00",DEF,Exchange1,"2020-12-20 10:10:00",2,20.12,120
"2020-12-20 14:00:00","2020-12-20 00:00:00",DEF,Exchange1,"2020-12-20 10:20:00",3,21.12,125
"2020-12-20 15:00:00","2020-12-20 00:00:00",DEF,Exchange1,"2020-12-20 10:30:00",4,21.12,125
"2020-12-21 12:00:00","2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:00:00",1,24.12,127
"2020-12-21 13:00:00","2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:10:00",2,21.21,125
"2020-12-21 14:00:00","2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:20:00",3,23.11,125
"2020-12-21 15:00:00","2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:30:00",4,21.12,125
"2020-12-21 12:00:00","2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:00:00",1,26.12,125
"2020-12-21 13:00:00","2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:10:00",2,25.12,121
"2020-12-21 14:00:00","2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:20:00",3,22.12,120
"2020-12-21 15:00:00","2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:30:00",4,21.12,125

```
* Step 2: Aggreates trade prices within ***30-minutes sliding windows*** from `v1` and save the result in a temp table `t1`. The `t1` columns are: `symbol`, `exchange`, `event_time`, `event_seq_num`, `trade_price`, `running_avg`. `running_avg` is the average trade prices within a 30-minutes window.

SQL statement:
```sql
DROP TABLE IF EXISTS moving_avg_tb;
CREATE TEMPORARY TABLE moving_avg_tb AS
SELECT trade_dt,
       symbol, 
       exchange, 
       event_time, 
       event_seq_num, 
       trade_price,
       AVG(trade_price) OVER (PARTITION BY symbol ORDER BY event_time
				 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_avg
FROM trade
WHERE trade_dt = '2020-12-21'
;
```

An sample output of `t1`:

```
trade_dt,symbol,exchange,event_time,event_seq_num,trade_price,running_avg
"2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:00:00",1,24.12,24.1200008392334
"2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:10:00",2,21.21,22.664999961853027
"2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:20:00",3,23.11,22.81333351135254
"2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:30:00",4,21.12,21.81333351135254
"2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:00:00",1,26.12,26.1200008392334
"2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:10:00",2,25.12,25.6200008392334
"2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:20:00",3,22.12,24.45333417256673
"2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:30:00",4,21.12,22.786667505900066

```

* Step 3: Repeat step 1 and step 2 for ***the day before*** of the current day and save the result in a temp table `t2`.

SQL statements:
```
DROP TABLE IF EXISTS last_moving_avg_tb;
CREATE TEMPORARY TABLE last_moving_avg_tb AS
SELECT  symbol, 
		exchange, 
		event_time, 
        event_seq_num, 
        trade_price,
		AVG(trade_price) OVER (PARTITION BY symbol ORDER BY event_time
				 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_pr
FROM trade
WHERE trade_dt = '2020-12-20'
;
```
The output:
```sql
symbol,exchange,event_time,event_seq_num,trade_price,last_pr
ABC,Exchange1,"2020-12-20 10:30:00",4,21.12,20.786667505900066
ABC,Exchange1,"2020-12-20 10:20:00",3,21.12,20.45333417256673
ABC,Exchange1,"2020-12-20 10:10:00",2,20.12,20.1200008392334
ABC,Exchange1,"2020-12-20 10:00:00",1,20.12,20.1200008392334
DEF,Exchange1,"2020-12-20 10:30:00",4,21.12,20.786667505900066
DEF,Exchange1,"2020-12-20 10:20:00",3,21.12,20.45333417256673
DEF,Exchange1,"2020-12-20 10:10:00",2,20.12,20.1200008392334
DEF,Exchange1,"2020-12-20 10:00:00",1,20.12,20.1200008392334
```

Next, get the last avg moving price for each partition of symbol

```sql
DROP TABLE IF EXISTS last_moving_avg_tb;
CREATE TEMPORARY TABLE last_moving_avg_tb AS
SELECT  symbol, 
		exchange, 
		event_time, 
        event_seq_num, 
        trade_price,
		LAST_VALUE(avg_pr) 
           OVER (PARTITION BY symbol 
                 ORDER BY event_time
                 RANGE BETWEEN
                    UNBOUNDED PRECEDING AND
                    UNBOUNDED FOLLOWING
                 ) AS last_pr
FROM prev_moving_avg_tb
;
```
The output:
```
symbol,exchange,event_time,event_seq_num,trade_price,last_pr
ABC,Exchange1,"2020-12-20 10:00:00",1,20.12,22.880000432332356
ABC,Exchange1,"2020-12-20 10:10:00",2,22.32,22.880000432332356
ABC,Exchange1,"2020-12-20 10:20:00",3,21.2,22.880000432332356
ABC,Exchange1,"2020-12-20 10:30:00",4,25.12,22.880000432332356
DEF,Exchange1,"2020-12-20 10:00:00",1,20.12,20.786667505900066
DEF,Exchange1,"2020-12-20 10:10:00",2,20.12,20.786667505900066
DEF,Exchange1,"2020-12-20 10:20:00",3,21.12,20.786667505900066
DEF,Exchange1,"2020-12-20 10:30:00",4,21.12,20.786667505900066

```

* Step 4: Load ***quote*** data for the current day into a temp view `v2`. Remind that the column in `v2` are: `arrival_time`, `trade_dt`, `symbol`, `exchange`, `event_time`, `event_seq_num`, `bid_price`, `bid_size`, `ask_price`, `ask_size`.

```
SELECT * FROM quote WHERE trade_dt = '2020-12-21';
```

The output:
```
arrival_time,trade_dt,symbol,exchange,event_time,event_seq_num,bid_price,bid_size,ask_price,ask_size
"2020-12-21 12:00:00","2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:00:00",1,20.12,120,21.57,110
"2020-12-21 13:00:00","2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:10:00",2,20.12,120,20.57,111
"2020-12-21 14:00:00","2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:20:00",3,21.12,125,19.57,113
"2020-12-21 15:00:00","2020-12-21 00:00:00",ABC,Exchange1,"2020-12-21 10:30:00",4,21.12,125,19.57,113
"2020-12-21 12:00:00","2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:00:00",1,20.12,120,21.57,110
"2020-12-21 13:00:00","2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:10:00",2,20.12,120,20.57,111
"2020-12-21 14:00:00","2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:20:00",3,21.12,125,19.57,113
"2020-12-21 15:00:00","2020-12-21 00:00:00",DEF,Exchange1,"2020-12-21 10:30:00",4,21.12,125,19.57,113
```

* Union `t1` and `v2` with the ***common schema***:

Column | Value
-------|---------
trade_dt | Value from corresponding records
rec_type | “Q” for quotes, “T” for trades
symbol | Value from corresponding records
event_tm | Value from corresponding records
event_seq_nb | From quotes, null for trades
exchange  | Value from corresponding records
bid_pr | From quotes, null for trades
bid_size | From quotes, null for trades
ask_pr | From quotes, null for trades
ask_size | From quotes, null for trades
trade_pr | From trades, null for quotes
mov_avg_pr | From trades, null for quotes

```
DROP TABLE IF EXISTS quote_union;
CREATE TABLE IF NOT EXISTS quote_union(
trade_dt datetime,
rec_type varchar(8),
symbol varchar(8),
event_time datetime,
event_seq_num int NULL,
exchange varchar(32),
bid_price float NULL,
bid_size int NULL,
ask_price float NULL,
ask_size int NULL,
trade_price float NULL,
mov_avg_pr float NULL
);

INSERT INTO quote_union (
SELECT trade_dt, 'T', symbol, event_time, NULL, exchange, NULL, NULL, NULL, NULL, trade_price, running_avg
FROM moving_avg_tb
UNION
SELECT trade_dt, 'Q', symbol, event_time, event_seq_num, exchange, bid_price, bid_size, ask_price, ask_size, NULL, NULL 
FROM quote
WHERE trade_dt = '2020-12-21'
)
;
```

The output:
```
trade_dt,rec_type,symbol,event_time,event_seq_num,exchange,bid_price,bid_size,ask_price,ask_size,trade_price,mov_avg_pr
"2020-12-21 00:00:00",T,ABC,"2020-12-21 10:30:00",NULL,Exchange1,NULL,NULL,NULL,NULL,21.12,21.8133
"2020-12-21 00:00:00",T,ABC,"2020-12-21 10:20:00",NULL,Exchange1,NULL,NULL,NULL,NULL,23.11,22.8133
"2020-12-21 00:00:00",T,ABC,"2020-12-21 10:10:00",NULL,Exchange1,NULL,NULL,NULL,NULL,21.21,22.665
"2020-12-21 00:00:00",T,ABC,"2020-12-21 10:00:00",NULL,Exchange1,NULL,NULL,NULL,NULL,24.12,24.12
"2020-12-21 00:00:00",T,DEF,"2020-12-21 10:30:00",NULL,Exchange1,NULL,NULL,NULL,NULL,21.12,22.7867
"2020-12-21 00:00:00",T,DEF,"2020-12-21 10:20:00",NULL,Exchange1,NULL,NULL,NULL,NULL,22.12,24.4533
"2020-12-21 00:00:00",T,DEF,"2020-12-21 10:10:00",NULL,Exchange1,NULL,NULL,NULL,NULL,25.12,25.62
"2020-12-21 00:00:00",T,DEF,"2020-12-21 10:00:00",NULL,Exchange1,NULL,NULL,NULL,NULL,26.12,26.12
"2020-12-21 00:00:00",Q,ABC,"2020-12-21 10:00:00",1,Exchange1,20.12,120,21.57,110,NULL,NULL
"2020-12-21 00:00:00",Q,ABC,"2020-12-21 10:10:00",2,Exchange1,20.12,120,20.57,111,NULL,NULL
"2020-12-21 00:00:00",Q,ABC,"2020-12-21 10:20:00",3,Exchange1,21.12,125,19.57,113,NULL,NULL
"2020-12-21 00:00:00",Q,ABC,"2020-12-21 10:30:00",4,Exchange1,21.12,125,19.57,113,NULL,NULL
"2020-12-21 00:00:00",Q,DEF,"2020-12-21 10:00:00",1,Exchange1,20.12,120,21.57,110,NULL,NULL
"2020-12-21 00:00:00",Q,DEF,"2020-12-21 10:10:00",2,Exchange1,20.12,120,20.57,111,NULL,NULL
"2020-12-21 00:00:00",Q,DEF,"2020-12-21 10:20:00",3,Exchange1,21.12,125,19.57,113,NULL,NULL
"2020-12-21 00:00:00",Q,DEF,"2020-12-21 10:30:00",4,Exchange1,21.12,125,19.57,113,NULL,NULL
```

# sb-guided-capstone
Springboard guided capstone project

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
Note that different record types (`T` for trade and `Q` for quote) have different line format.
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
* Step 2: Aggreates trade prices within ***30-minutes sliding windows*** from `v1` and save the result in a temp table `t1`. The `t1` columns are: `symbol`, `exchange`, `event_time`, `event_seq_num`, `trade_price`, `running_avg`. `running_avg` is the average trade prices within a 30-minutes window.
* Step 3: Repeat step 1 and step 2 for ***the day before*** of the current day and save the result in a temp table `t2`. The `t2` columns are: `symbol`, `exchange`, `last_price`. `last_price` is the average trade price within 30-minutes of the last window of the previous day.
* Step 4: Load ***quote*** data for the current day into a temp view `v2`. Remind that the column in `v2` are: `arrival_time`, `trade_dt`, `symbol`, `exchange`, `event_time`, `event_seq_num`, `bid_price`, `bid_size`, `ask_price`, `ask_size`.
* Join `t1` and `v2`

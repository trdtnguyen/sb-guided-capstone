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

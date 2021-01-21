from datetime import datetime
import psycopg2
from controllers.GlobalUtil import GlobalUtil
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import logging
import sys


class Tracker(object):
    def __init__(self, jobname):
        self.jobname = jobname
        self.GU = GlobalUtil.instance()

        app_name = self.GU.CONFIG['CORE']['APP_NAME']
        self.spark = SparkSession \
            .builder \
            .master('local') \
            .appName(app_name) \
            .getOrCreate()
        logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                            format='%(levelname)s - %(message)s')
        self.table_name = self.GU.CONFIG['DATABASE']['JOB_TRACKER_TABLE_NAME']
        self.is_new_id = True

    def assign_job_id(self):
        try:
            df = self.GU.read_from_db(self.spark, self.table_name)
            df_filter = df.filter(df['job_name'] == self.jobname)
            if df_filter.count() == 0:
                # generate next job id
                last_id = df.select(df['job_id']).rdd.max()[0]
                return last_id + 1
            else:
                job_id = df_filter.select(['job_id']).rdd.flatMap(lambda x: x).collect()[0]
                self.is_new_id = False
                return job_id
        except Exception:
            logging.error("Read database error in assign_job_id()")
            return None

    def get_job_status(self, job_id: int):
        try:
            df = self.GU.read_from_db(self.spark, self.table_name)
            status = df.filter(df['job_id'] == job_id).select(df['status']).rdd.flatMap(lambda x: x).collect()[0]
            return status
        except Exception:
            logging.error("Read database error in get_job_status()")
            return None

    def update_job_status(self, status: str):
        job_id = self.assign_job_id()
        update_time = datetime.now()
        try:
            # read from database
            df = self.GU.read_from_db(self.spark, self.table_name)
            # make spark actually load table in memory
            df = df.cache()
            df.count()

            if self.is_new_id:
                # insert new row
                cols=['job_id', 'job_name', 'status', 'updated_time']
                newRow = self.spark.createDataFrame([(
                    job_id, self.jobname, status, update_time)], cols)
                # Write on database with append mode
                print(f"Write on table {self.table_name}...", end=' ')
                self.GU.write_to_db(newRow, self.table_name, 'append', logging)
                print('Done.')
            else:
                # update status and update_time based on job_id
                df = df.withColumn('status',
                                   when(df['job_id'] == job_id, status).otherwise(df['status']))\
                    .withColumn('updated_time',
                                when(df['job_id'] == job_id, update_time).otherwise(df['updated_time']))

                # Write on database with overwrite mode
                print(f"Write on table {self.table_name}...", end= ' ')
                self.GU.write_to_db(df, self.table_name, 'overwrite', logging)
                print('Done.')

        except Exception:
            logging.error("Read database error in get_job_status()")




t = Tracker("job4")
t.update_job_status("not ok")

t2 = Tracker("job3")
t2.update_job_status("not ok")
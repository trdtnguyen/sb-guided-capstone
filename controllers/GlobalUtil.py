"""
Global Utils and Constant
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'


import sys
import os
from os import environ as env
from datetime import datetime
import configparser
import logging


class GlobalUtil(object):
    # for Singleton usage
    _instance = None

    # Class attributes as global const
    JOB_TRACKER_TABLE_NAME = 'job_tracker'

    PROJECT_PATH = env.get('PROJECT_PATH')
    if PROJECT_PATH is None:
        print("===> PROJECT_PATH is not set. Get project path from os.path")
        PROJECT_PATH = os.path.join(os.path.dirname(__file__),"..")

    CONFIG = configparser.RawConfigParser()  # Use RawConfigParser() to read url with % character
    CONFIG_FILE = 'config.cnf'
    config_path = os.path.join(PROJECT_PATH, CONFIG_FILE)
    CONFIG.read(config_path)

    JDBC_PG_URL = 'jdbc:postgresql://' + CONFIG['DATABASE']['HOST'] + ':' + \
                     CONFIG['DATABASE']['PORT'] + '/' + \
                     CONFIG['DATABASE']['DATABASE'] + '?' + \
                     'rewriteBatchedStatements=true'
    # DRIVER_NAME = 'com.mysql.cj.jdbc.Driver'

    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(levelname)s - %(message)s')


    def __init__(self):
        raise RuntimeError('Call instance() instead')

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)
            # Init

        return cls._instance


    @classmethod
    def read_from_db(cls, spark, in_table_name):
        df = spark.read.format('jdbc').options(
            url=cls.JDBC_PG_URL,
            dbtable=in_table_name,
            user=cls.CONFIG['DATABASE']['PG_USER'],
            password=cls.CONFIG['DATABASE']['PG_PASSWORD']).load()
        return df

    @classmethod
    def write_to_db(cls, df, table_name, write_mode, logger):
        try:
            df.write.format('jdbc').options(
                url=cls.JDBC_PG_URL,
                dbtable=table_name,
                user=cls.CONFIG['DATABASE']['PG_USER'],
                password=cls.CONFIG['DATABASE']['PG_PASSWORD']).mode(write_mode).save()
        except ValueError:
            logger.error(f'Error Query when extracting data for {table_name} table')



    """
    Covert from rows selected by col_name to array
    """

    @classmethod
    def rows_to_array(cls, df, col_name: str):
        rdd = df.select(df[col_name]).rdd
        arr = rdd.flatMap(lambda row: row).collect()
        return arr

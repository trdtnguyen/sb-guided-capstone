import psycopg2

from os import environ as env
import os
import configparser


project_path = os.path.join(os.path.dirname(__file__),"..")
CONFIG = configparser.RawConfigParser()  # Use RawConfigParser() to read url with % character
CONFIG_FILE = 'config.cnf'
config_path = os.path.join(project_path, CONFIG_FILE)
CONFIG.read(config_path)

dbname = CONFIG['DATABASE']['PGDATABASE']
user = CONFIG['DATABASE']['PGUSER']
password = CONFIG['DATABASE']['PGPASSWORD']
host = CONFIG['DATABASE']['PGHOST']
hostaddr = CONFIG['DATABASE']['PGHOSTADDR']
try:
    conn_str = f"dbname='{dbname}' user='{user}' host='{host}' port=5432 password='{password}'"
    #conn_str = f"dbname='{dbname}' user='{user}' hostaddr='{hostaddr}' port=5432 password='{password}'"
    print(f'conn_str={conn_str}')
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    print('create tables ...', end=' ')
    sql_cmd=""
    with open(os.path.join(project_path,'sql/create_table.sql')) as f:
        lines = f.readlines()
        for line in lines:
            sql_cmd = sql_cmd + " " + line
            if ';' in line:
                # print(sql_cmd)
                cursor.execute(sql_cmd)
                sql_cmd = ""

    cursor.close()
    conn.commit() # this is important
    conn.close()
    print('Done.')



except Exception as error:
    print("Error while connecting to database for job tracker", error)

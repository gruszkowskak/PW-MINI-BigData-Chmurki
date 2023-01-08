import sys
import re
import happybase
import sh
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkDemo").getOrCreate()
date_param = (sys.argv[1])
date = datetime.strptime(date_param, '%Y-%m-%d')
date_prew = datetime.strptime(date_param, '%Y-%m-%d') - timedelta(1)
date_str = datetime.strftime(date, '%Y-%m-%d %H:%M:%S')
date_prew_str = datetime.strftime(date_prew, '%Y-%m-%d %H:%M:%S')
hdfsdir = 'hdfs://localhost:8020/user/project/alphavantage'
files_names = [ line.rsplit(None,1)[-1] for line in sh.hdfs('dfs','-ls',hdfsdir).split('\n') if len(line.rsplit(None,1))][1:]

def search_file(files_names,regex):
  for file_name in files_names:
    match = re.search(regex, file_name)
    if (match):
      return str(match.group())

regex_AAPL = r'hdfs://localhost:8020/user/project/alphavantage/AAPL_' + date_param + r'.*'
regex_TSLA = r'hdfs://localhost:8020/user/project/alphavantage/TSLA_' + date_param + r'.*'
regex_MSFT = r'hdfs://localhost:8020/user/project/alphavantage/MSFT_' + date_param + r'.*'
regex_GOOG = r'hdfs://localhost:8020/user/project/alphavantage/GOOG_' + date_param + r'.*'

path_AAPL = search_file(files_names,regex_AAPL)
path_TSLA = search_file(files_names,regex_TSLA)
path_MSFT = search_file(files_names,regex_MSFT)
path_GOOG = search_file(files_names,regex_GOOG)

AAPL=spark.read.parquet(path_AAPL)
TSLA=spark.read.parquet(path_TSLA)
MTFS=spark.read.parquet(path_MSFT)
GOOG=spark.read.parquet(path_GOOG)

end_datetime = date_str
start_datetime = date_prew_str

AAPL= AAPL.filter((AAPL['timestamp']<end_datetime) &  (AAPL['timestamp'] > start_datetime) )
GOOG= GOOG.filter((GOOG['timestamp']<end_datetime) &  (GOOG['timestamp'] > start_datetime) )
MSFT= MTFS.filter((MTFS['timestamp']<end_datetime) &  (MTFS['timestamp'] > start_datetime) )
TSLA= TSLA.filter((TSLA['timestamp']<end_datetime) &  (TSLA['timestamp'] > start_datetime) )

AAPL = AAPL.withColumnRenamed("open","AAPL_open").withColumnRenamed("high","AAPL_high").withColumnRenamed("low","AAPL_low").withColumnRenamed("close","AAPL_close").withColumnRenamed("volume","AAPL_volume")
GOOG = GOOG.withColumnRenamed("open","GOOG_open").withColumnRenamed("high","GOOG_high").withColumnRenamed("low","GOOG_low").withColumnRenamed("close","GOOG_close").withColumnRenamed("volume","GOOG_volume")
MSFT = MSFT.withColumnRenamed("open","MSFT_open").withColumnRenamed("high","MSFT_high").withColumnRenamed("low","MSFT_low").withColumnRenamed("close","MSFT_close").withColumnRenamed("volume","MSFT_volume")
TSLA = TSLA.withColumnRenamed("open","TSLA_open").withColumnRenamed("high","TSLA_high").withColumnRenamed("low","TSLA_low").withColumnRenamed("close","TSLA_close").withColumnRenamed("volume","TSLA_volume")

ALPHAVENTAGE = AAPL.join(GOOG, 'timestamp','full').join(MSFT, 'timestamp','full').join(TSLA, 'timestamp','full')

batch_size = 1000
host = "localhost"
row_count = 0
table_name = "alphavantage"
def connect_to_hbase():
    conn = happybase.Connection(host = host)
    conn.open()
    table = conn.table(table_name)
    batch = table.batch(batch_size = batch_size)
    return conn, batch

def insert_row(batch, row):
    batch.put(row[0],       {b'Id:Time' : str(row.timestamp),

                            b'GOOG:open': str(row.GOOG_open),
                            b'GOOG:close': str(row.GOOG_close),
                            b'GOOG:low': str(row.GOOG_low),
                            b'GOOG:high': str(row.GOOG_high),
                            b'GOOG:volume': str(row.GOOG_volume),

                            b'MSFT:open': str(row.MSFT_open),
                            b'MSFT:close': str(row.MSFT_close),
                            b'MSFT:low': str(row.MSFT_low),
                            b'MSFT:high': str(row.MSFT_high),
                            b'MSFT:volume': str(row.MSFT_volume),

                            b'AAPL:open': str(row.AAPL_open),
                            b'AAPL:close': str(row.AAPL_close),
                            b'AAPL:low': str(row.AAPL_low),
                            b'AAPL:high': str(row.AAPL_high),
                            b'AAPL:volume': str(row.AAPL_volume),

                            b'TSLA:open': str(row.TSLA_open),
                            b'TSLA:close': str(row.TSLA_close),
                            b'TSLA:low': str(row.TSLA_low),
                            b'TSLA:high': str(row.TSLA_high),
                            b'TSLA:volume': str(row.TSLA_volume)
                            })

conn, batch = connect_to_hbase()
try:
    for row in ALPHAVENTAGE.collect():
        insert_row(batch, row)
    batch.send()
finally:
    conn.close()





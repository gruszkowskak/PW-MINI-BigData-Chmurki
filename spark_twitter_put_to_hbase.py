import sys
import re
import happybase
import sh
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
from functools import reduce


spark = SparkSession.builder.appName("SparkDemo").getOrCreate()
date_param = (sys.argv[1])
date = datetime.strptime(date_param, '%Y-%m-%d')
date_prew = datetime.strptime(date_param, '%Y-%m-%d') - timedelta(1)
date_str = datetime.strftime(date, '%Y-%m-%d %H:%M:%S')
date_prew_str = datetime.strftime(date_prew, '%Y-%m-%d %H:%M:%S')
hdfsdir = 'hdfs://localhost:8020/user/project/twitter'
files_names = [ line.rsplit(None,1)[-1] for line in sh.hdfs('dfs','-ls',hdfsdir).split('\n') if len(line.rsplit(None,1))][1:]

def search_files(files_names,regex):
  files = []
  for file_name in files_names:
    match = re.search(regex, file_name)
    if (match):
      files.append(str(match.group()))
  return files

def data_from_full_day(company_name,date_param,files_names):
    regex = r'hdfs://localhost:8020/user/project/twitter/' + company_name+ r'_' + date_param + r'.*'
    files_paths = search_files(files_names,regex)
    dataframes=[]
    for file_path in files_paths:
        fd = spark.read.parquet(file_path)
        dataframes.append(fd)
    union_df = reduce(DataFrame.unionAll, dataframes)
    return union_df

AAPL=data_from_full_day("Apple_Hashtag",date_param,files_names)
TSLA=data_from_full_day("Tesla_Hashtag",date_param,files_names)
MSFT=data_from_full_day("Microsoft_Hashtag",date_param,files_names)
GOOG=data_from_full_day("Google_Hashtag",date_param,files_names)
AAPL = AAPL.withColumnRenamed("tweet_count", "AAPL_tweet_count")
GOOG = GOOG.withColumnRenamed("tweet_count", "GOOG_tweet_count")
MSFT = MSFT.withColumnRenamed("tweet_count", "MSFT_tweet_count")
TSLA = TSLA.withColumnRenamed("tweet_count", "TSLA_tweet_count")
TWITTER = AAPL.join(GOOG, 'end', 'full').join(MSFT, 'end', 'full').join(TSLA, 'end', 'full')

def parse(timestamp):
    minute = timestamp.split(':')[1]
    minute = int(minute) // 5 * 5
    new_timestamp = timestamp.split(':')[0] + ":" + str(minute) + ":" + timestamp.split(':')[2]
    date = new_timestamp.split('T')[0]
    hour = new_timestamp.split('T')[1]
    hour = hour[0:8]
    new_timestamp = date + " " + hour
    return new_timestamp

parse_col = udf(lambda x : parse(x), StringType())
TWITTER = TWITTER.withColumn("end5",parse_col(col("end")))
TWITTER = TWITTER.groupBy('end5').agg({'AAPL_tweet_count': 'sum','GOOG_tweet_count': 'sum','MSFT_tweet_count': 'sum','TSLA_tweet_count': 'sum'})
batch_size = 1000
host = "localhost"
row_count = 0
table_name = "twitter"
def connect_to_hbase():
    conn = happybase.Connection(host = host)
    conn.open()
    table = conn.table(table_name)
    batch = table.batch(batch_size = batch_size)
    return conn, batch

def insert_row(batch, row):
    batch.put(str(row.end5), {b'Id:Time' : str(row.end5),
                             b'Hashtags:GOOG_tweet_count': str(row["sum(GOOG_tweet_count)"]),
                             b'Hashtags:MSFT_tweet_count': str(row["sum(MSFT_tweet_count)"]),
                             b'Hashtags:AAPL_tweet_count': str(row["sum(AAPL_tweet_count)"]),
                             b'Hashtags:TSLA_tweet_count': str(row["sum(TSLA_tweet_count)"])
                            })

conn, batch = connect_to_hbase()
try:
    for row in TWITTER.collect():
        insert_row(batch, row)
    batch.send()
finally:
    conn.close()

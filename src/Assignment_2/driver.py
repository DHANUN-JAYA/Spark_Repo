from utils import *
import pyspark
from pyspark.sql import SparkSession

sc = create_SparkConf()

#loading textFile in RDD
path="C:/Users/Welcome/PycharmProjects/Spark_Repo/Spark_Repo/resourses/ghtorrent-logs.txt"
parsedRDD=getRDD(sc,path)

#How many lines does the RDD contain
count_rowrdd=count_rowrdd(parsedRDD)
print(count_rowrdd)
#count no of WARN messages in rdd
count_warn=no_warn_rdd(parsedRDD)
print(count_warn)
#How many repositories where processed in total? Use the api_client lines only.
no_api_client=api_clint(parsedRDD)
print(no_api_client)
#Which client did most HTTP requests
max_client=HTTP(parsedRDD)
print(max_client)
#Which client did most FAILED HTTP requests? Use group_by to provide an answer.
failed_client=failed_HTTP(parsedRDD)
print(failed_client)
#What is the most active hour of day?
most_active_hours=hours_count(parsedRDD)
print(most_active_hours)
#What is the most active repository
active_repo=active_Repo(parsedRDD)
print(active_repo)
# stop the session
stop(sc)


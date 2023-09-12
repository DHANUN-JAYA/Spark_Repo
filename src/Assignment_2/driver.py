from utils import *
import pyspark
from pyspark.sql import SparkSession

sc = create_SparkConf()



#loading textFile in RDD

parsedRDD=getRDD(sc)

#How many lines does the RDD contain
count_rowrdd=count_rowrdd(parsedRDD)

#count no of WARN messages in rdd
count_warn=no_warn_rdd(parsedRDD)

#How many repositories where processed in total? Use the api_client lines only.
no_api_client=api_clint(parsedRDD)

#Which client did most HTTP requests
max_client=HTTP(parsedRDD)

#Which client did most FAILED HTTP requests? Use group_by to provide an answer.
failed_client=failed_HTTP(parsedRDD)

#What is the most active hour of day?
most_active_hours=hours_count(parsedRDD)

#What is the most active repository
active_repo=active_Repo(parsedRDD)




import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum
import logging
logging.basicConfig(filename="../../resourses/logfile_Assignment_1", filemode="w")
log = logging.getLogger()
log.setLevel(logging.INFO)
def create_session():
    log.info("Session for spark created")
    return  SparkSession.builder.appName('Spark_Assignment_1').getOrCreate()
def read(sc,path,boolean):
    log.info(f"Reading dataframe for {path}")
    return sc.read.csv(path,header=boolean)

def merge(df_user,df_transaction):
    total_df=total_df = df_user.join(df_transaction, df_user.user_id == df_transaction.userid )
    log.info("combined dataframe for user and transaction on user_id")
    return total_df
def count_unique_locations(total_df):
     # a) Count of unique locations where each product is sold.
     log.info("count_unique_locations method execution")
     return total_df.groupBy("location ").agg(countDistinct("product_description").alias("product_Count"))
def products_bought(total_df):
    # b) Find  out products bought by each user.
    log.info("products_bought method execution")
    return total_df.groupBy("user_id").agg({"product_description": "collect_list"})
def total_spending(total_df):
    # c) Total spending done by each user on each product.
    log.info("total_spending method execution")
    return total_df.groupBy(["user_id"]).agg(sum("price").alias("Total_Spending"))
def stop(sc):
    return sc.stop()
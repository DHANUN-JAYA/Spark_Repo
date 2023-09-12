import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum
import logging
def create_session():
    return  SparkSession.builder.appName('Spark_Assignment_1').getOrCreate()
def stop(sc):
    return sc.stop()
def read(sc,path,boolean):
    return sc.read(path,header=boolean)
def merge(df_user,df_transaction):
    total_df=total_df = df_user.join(df_transaction, df_user.user_id == df_transaction.userid )
    return total_df
def count_unique_locations(total_df):
     # a) Count of unique locations where each product is sold.
     return total_df.groupBy("location ").agg(countDistinct("product_description").alias("product_Count"))
def products_bought(total_df):
    # b) Find  out products bought by each user.
    return total_df.groupBy("user_id").agg({"product_description": "collect_list"})
def total_spending(total_df):
    # c) Total spending done by each user on each product.
    return total_df.groupBy(["user_id"]).agg(sum("price").alias("Total_Spending"))

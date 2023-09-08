import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum
spark=SparkSession.builder.appName('Spark_Assignment').getOrCreate()
df_user=spark.read.csv('C:/Users/Welcome/Desktop/user.csv',header=True)

df_transaction=spark.read.csv('C:/Users/Welcome/Desktop/transaction.csv',header=True)

total_df = df_user.join(df_transaction, df_user.user_id == df_transaction.userid,)
# a) Count of unique locations where each product is sold.
total_df.groupBy("location ").agg(countDistinct("product_description").alias("product_Count")).show()

# b) Find out products bought by each user.
total_df.groupBy("user_id").agg({"product_description":"collect_list"}).show()

# c) Total spending done by each user on each product.
user_product_spending = total_df.groupBy(["user_id"]).agg(sum("price").alias("Total_Spending")).show()
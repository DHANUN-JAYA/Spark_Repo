from utils import *
import pyspark
from pyspark.sql import SparkSession

#creating spark session
spark=create_session()

#loading user_csv  file
df_user =read(spark,'C:/Users/Welcome/PycharmProjects/Spark_Repo/Spark_Repo/resourses/user.csv', True)

#loading transaction_csv file
df_transaction = read(spark,'C:/Users/Welcome/PycharmProjects/Spark_Repo/Spark_Repo/resourses/transaction.csv', True)

#merging  user_csv and transaction_csv file
total_df = merge(df_user,df_transaction,)

# a) Count of unique locations where each product is sold.
count_unique_locations_df=count_unique_locations(total_df)
# b) Find out products bought by each user.
products_bought_df=products_bought(total_df)
# c) Total spending done by each user on each product.
total_spending_df=total_spending(total_df)
# stop the session
stop(spark)
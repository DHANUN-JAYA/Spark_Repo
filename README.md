Spark_Assignments:
Question_1:
•	Imported SparkSession from pyspark.sql lib
•	In the resources directory imported both user and transaction csv files
•	Created a function to create SparkSession (create_session())
•	Added log files
•	To read the both files user and transaction with path created a (read()), which return the user_df and transaction_df 
•	To merge both dataframes user_df and transaction_df on user_id column created a (merge()), which return the merged_df 
a) Count of unique locations where each product is sold
•	total_df i.e  merged dataframe is passed to the function count_unique_location(total_df))
•	It calculates the count_unique_locations where each product is sold by grouping the DataFrame by the 'location' column and aggregating the distinct count of 'product_description.
b) Find out products bought by each user.
•	The function products_bought(total_df) takes a DataFrame (total_df) as input.
•	It finds out the products bought by each user by grouping the DataFrame by 'user_id' and aggregating a list of 'product_description.'
 c) Total spending done by each user on each product.
•	The function total_spending(total_df) takes a DataFrame (total_df) as input.
•	It calculates the total spending done by each user on each product by grouping the DataFrame by 'user_id' and aggregating the sum of 'price.'
•	The result includes a 'Total_Spending' column.
The stop(sc) function takes the SparkSession (sc) as input and stops the Spark session when called.

-----------------------------------------------------------------------------------------------------------------------------
Question_2:
•Imported SparkContext from pyspark.sql
•create_SparkConf():
•	This function creates a SparkContext

1.loading textFile in RDD of logfile passed path to getRDD(sc, path) function
getRDD(sc, path):
•	This function takes a SparkContext (sc) and a file path (path) as inputs.
•	It reads a text file from the specified path and converts it into an RDD.
•	The myParse function is used to parse each line of the text file.
2.How many lines does the RDD contain
count_rowrdd(rowrdd):
•	This function takes an RDD (rowrdd) as input and counts the number of rows (lines) in the RDD.
•	It logs the number of lines in the RDD as both a warning and an error message.
3.count no of WARN messages in rdd
no_warn_rdd(rowrdd):
•	This function takes an RDD (rowrdd) as input and filters it to count the number of lines that contain the word "WARN" in the first position.
•	It logs the number of such lines as a warning message.
4.How many repositories where processed in total? Use the api_client lines only.
api_clint(rowrdd):
•	This function takes an RDD (rowrdd) as input and performs several operations:
•	It filters the RDD to keep only rows with exactly 5 elements.
•	It further filters the rows to include only those with "api_client" in the third position.
•	It extracts and processes repository information from these rows and groups them by repository.
•	It counts the unique repositories.
•	It logs the number of unique repositories as a warning message.
5.Which client did most HTTP requests
HTTP(rowrdd):
•	This function takes an RDD (rowrdd) as input and performs several operations:
•	It filters the RDD to keep only rows with exactly 5 elements.
•	It further filters the rows to include only those with "api_client" in the third position.
•	It groups the rows by the user and counts the number of times each user appears.
•	It finds the user with the maximum count (indicating the user with the most API calls).
•	It returns the user with the maximum count of API calls.
6.Which client did most FAILED HTTP requests? Use group_by to provide an answer.
failed_HTTP(rowrdd):
•	This function takes an RDD (rowrdd) as input and performs several operations:
•	It filters the RDD to keep only rows with exactly 5 elements.
•	It further filters the rows to include only those with "api_client" in the third position.
•	It filters rows with "Failed" HTTP requests.
•	It groups the rows by the user and counts the number of times each user had failed requests.
•	It finds the user with the maximum count of failed requests.
•	It returns the user with the maximum count of failed HTTP requests.
7.What is the most active hour of day?
hours_count(rowrdd):
•	This function takes an RDD (rowrdd) as input and performs several operations:
•	It filters the RDD to keep only rows with exactly 5 elements.
•	It extracts the hour of the day from the timestamp and adds it as a new column to each row.
•	It groups the rows by the hour and counts the number of events in each hour.
•	It finds the hour with the maximum count of events.
•	It returns the hour with the maximum count of events.
8.What is the most active repository
active_Repo(rowrdd):
•	This function takes an RDD (rowrdd) as input and performs several operations:
•	It filters the RDD to keep only rows with exactly 5 elements.
•	It further filters the rows to include only those with "api_client" in the third position.
•	It extracts and processes repository information from these rows.
•	It groups the rows by the repository and counts the number of events for each repository.
•	It finds the repository with the maximum count of events.
•	It returns the repository with the maximum count of events.
stop(sc):
•	This function takes the SparkContext (sc) as input and stops the Spark session.






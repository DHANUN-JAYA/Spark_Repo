import unittest
import pyspark
from pyspark.sql import SparkSession
from Spark_Repo.src.Assignment_2.utils import *
from pyspark.sql.types import *
class MyTestCase(unittest.TestCase):

    def test_something(self):
        sc = create_SparkConf()
        rdd = getRDD(sc)
        count_lines = 281234
        warn_ct = 3811
        api_ct = 16532
        # Testing the count of lines in rdd
        actual_input_count = count_rowrdd(rdd)
        expected_output = count_lines
        self.assertEqual(actual_input_count,  expected_output)

        # counting number of warn logs
        actual_input_warn_count = no_warn_rdd(rdd)
        expected_warn_count = warn_ct
        self.assertEqual(actual_input_warn_count, expected_warn_count)

        # Repositories count in apl_client
        actual_input_api_client = api_clint(rdd)
        expected_api_client = api_ct
        self.assertEqual(actual_input_api_client, expected_api_client)

        # Which client did most HTTP requests
        client_schema = StructType([
            StructField('clt_id', StringType(), True),
            StructField('count_total', IntegerType(), True)
        ])
        expected_http_max = ("13", 3983)
        actual_http_max=HTTP(rdd)
        self.assertEqual(actual_http_max,  expected_http_max)

        # Which client did most FAILED HTTP requests? Use group_by to provide an answer.
        client_schema = StructType([
            StructField('clt_id', StringType(), True),
            StructField('count_total', IntegerType(), True)
        ])
        expected_failed_http_max = ("13", 2321)
        actual_failed_http_max = failed_HTTP(rdd)
        self.assertEqual(actual_failed_http_max, expected_failed_http_max)

        # What is the most active hour of day?
        expected_hours_count = ("10", 78000)
        actual_hours_count= hours_count(rdd)
        self.assertEqual(expected_hours_count, actual_hours_count)

        # What is the most active repository
        expected_count_active_repo=('greatfakeman/Tabchi', 2318)
        actual_count_active_repo=active_Repo(rdd)
        self.assertEqual(expected_count_active_repo, actual_count_active_repo)
if __name__ == '__main__':
    unittest.main()

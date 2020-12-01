import unittest
#
import os
import pickle
from pyspark.sql.types import *

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["SPARK_HOME"] = "/usr/local/spark-2.4.7"

from pyspark.sql import SparkSession
from pyspark import SparkConf,SQLContext,HiveContext,SparkContext

#--------------------------------------------------------
import sys
import pymysql

class test_stock(unittest,testcase):
    #
    def test_firstcase(self):
    #connect to mysql database
        con = pymysql.connect(host='127.0.0.1',user='root',passwd='greatnaolAT0*',db='stock')
        cursor = con.cursor()

        cursor.execute("select date from goog_monthly");

        result = cursor.fetchall()

        cursor.execute ("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'msft_stock'");

        result1 = cursor.fetchall()
        col_len = result1[0][0]
        #
        #
        spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", '/hive/warehouse') \
        .enableHiveSupport() \
        .getOrCreate()

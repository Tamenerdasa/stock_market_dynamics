from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

import os

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["SPARK_HOME"] = "/usr/local/spark-2.4.7"

kafka_topic_name = "msfttopic"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Welcome to Kafka !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", '/hive/warehouse') \
        .enableHiveSupport() \
        .getOrCreate()
    #
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    spark.sparkContext.setLogLevel("ERROR")

    # Constructing a streaming DataFrame that reads from test-topic
    stock_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()
    #
    print("Printing Schema of stock_df: ")
    stock_df.printSchema()
    #stock_df.show()
    stock_df1 = stock_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Defining a schema for the stock data
    stock_schema = StructType() \
        .add("date", StringType()) \
        .add("open", StringType()) \
        .add("high", StringType()) \
        .add("low", StringType()) \
        .add("close", StringType()) \
        .add("volume", StringType())
    #
    stock_df2 = stock_df1\
        .select(from_json(F.col("value"), stock_schema)\
        .alias("stock"), "timestamp")
    #
    stock_df3 = stock_df2.select("stock.*", "timestamp")
    # Writing final result into console for debugging purpose
    stock_agg_write_stream = stock_df3 \
       .writeStream \
       .trigger(processingTime='5 seconds') \
       .outputMode("update") \
       .option("truncate", "false")\
       .format("console") \
       .start()
    #
    def handle_hive(df, batch_id):
        df.write.saveAsTable(name='stock.msft_stock', format='hive', mode='append')
    #
    query5 = stock_df3.writeStream.foreachBatch(handle_hive).start()
    #query5.awaitTermination()
##-----------------------------------------------------------------------
    def handle_hive_rdbms(df, batch_id):
        df.write.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/stock?useSSL=false") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", "msft_stock") \
        .option("user", "++++++") \
        .option("password", "******") \
        .mode("append") \
        .save()
    query6 = stock_df3.writeStream.foreachBatch(handle_hive_rdbms).start()
    query6.awaitTermination()
    #

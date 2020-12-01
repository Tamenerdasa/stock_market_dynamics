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

    # Construct a streaming DataFrame that reads from test-topic
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

    # Define a schema for the stock data
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
    #
    # Simple aggregate - find total_order_amount by grouping country, city
    stock_df4 = stock_df3.withWatermark("timestamp", "1 minute").groupBy("date","open","high","low","close","volume","timestamp").agg({'close': 'avg'})
    stock_df5 = stock_df4.select("date","open","high","low","close","volume","timestamp", F.col("avg(close)").alias("rolling_avg"))
    stock_df5.printSchema()
    #stock_df4 = stock_df3.groupBy("order_country_name", "order_city_name") \
    #    .agg({'order_amount': 'sum'}) \
    #    .select("order_country_name", "order_city_name", F.col("sum(order_amount)") \
    #    .alias("total_order_amount"))
    #print("Printing Schema of stock_df4: ")
    #stock_df4.printSchema()
    # Write final result into console for debugging purpose
    stock_agg_write_stream = stock_df5 \
       .writeStream \
       .trigger(processingTime='5 seconds') \
       .outputMode("update") \
       .option("truncate", "false")\
       .format("console") \
       .start()
##---------------------------------------------------------------
    #spark.sql("CREATE TABLE IF NOT EXISTS stock.stocks_new (date string, open int, high int, \
    #    low int, close int, volume int")
    def handle_hive(df, batch_id):
        df.write.saveAsTable(name='stock.msft_stock', format='hive', mode='append')

    query5 = stock_df5.writeStream.foreachBatch(handle_hive).start()
    query5.awaitTermination()

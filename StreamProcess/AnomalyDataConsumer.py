# Author: Lee Ka Qin

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json

class AnomalyMonitor:
    def __init__(self, bootstrap_server="localhost:9092"):
        self.bootstrap_server = bootstrap_server
        self.topic = "Anomaly_Data"
        self.spark = (
            SparkSession
            .builder
            .appName("Live Anomaly Inspection")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.5")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        self.anomaly_path = "hdfs://localhost:9000/user/student/delivery_data/anomaly/"

    def start_monitoring(self):
        schemas = StructType([
            StructField('ID', StringType(), True),
            StructField('Delivery_person_ID', StringType(), True),
            StructField('Restaurant_latitude', StringType(), True),
            StructField('Restaurant_longitude', StringType(), True),
            StructField('Delivery_location_latitude', StringType(), True),
            StructField('Delivery_location_longitude', StringType(), True),
            StructField("Order_DateTime", StringType(), True),
            StructField("Picked_Datetime", StringType(), True),
            StructField("Delivered_Datetime", StringType(), True),
            StructField("Anomaly_Reason", StringType(), True) 
        ])

        streams_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_server) \
            .option("subscribe", self.topic) \
            .load()

        parsed_df = streams_df.select(
            from_json(col("value").cast("string"), schemas).alias("data")
        ).select("data.*")

        print("Anomaly Watchdog Online. Waiting for broken data...")
        
        query = parsed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()

        query.awaitTermination()

if __name__ == "__main__":
    monitor = AnomalyMonitor()
    monitor.start_monitoring()
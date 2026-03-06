from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import col, from_json, trim, to_date, to_timestamp, concat_ws

class SparkKafkaConsumer:
    def __init__(self, topic = "", bootstrap_server = "localhost:9092"):
        self.topic = topic
        self.bootstrap_server = bootstrap_server
        self.spark = (
            SparkSession
            .builder
            .appName("Streaming from Kafka")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.5")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        self.hdfs_output_path = "hdfs://localhost:9000/user/student/delivery_data/output/"
        self.hdfs_checkpoint_path = "hdfs://localhost:9000/user/student/delivery_data/checkpoints/"

    def streams_subscription(self):
        schemas = StructType([
            StructField('ID', StringType(), True),
            StructField('Delivery_person_ID', StringType(), True),
            StructField('Delivery_person_Age', StringType(), True),
            StructField('Delivery_person_Ratings', StringType(), True),
            StructField('Restaurant_latitude', StringType(), True),
            StructField('Restaurant_longitude', StringType(), True),
            StructField('Delivery_location_latitude', StringType(), True),
            StructField('Delivery_location_longitude', StringType(), True),
            StructField("Order_Date", StringType(), True),
            StructField("Time_Orderd", StringType(), True),
            StructField("Time_Order_picked", StringType(), True),
            StructField("Weatherconditions", StringType(), True),
            StructField("Road_traffic_density", StringType(), True),
            StructField("Vehicle_condition", StringType(), True),
            StructField("Type_of_order", StringType(), True),
            StructField("Type_of_vehicle", StringType(), True),
            StructField("multiple_deliveries", StringType(), True),
            StructField("Festival", StringType(), True),
            StructField("City", StringType(), True),
        ])

        streams_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_server) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load()

        parsed_df = streams_df.select(
            from_json(col("value").cast("string"), schemas ).alias("data")
        ).select("data.*")

        parsed_df = parsed_df \
            .withColumn("Delivery_person_Age", trim(col("Delivery_person_Age")).cast("integer")) \
            .withColumn("Delivery_person_Ratings", trim(col("Delivery_person_Ratings")).cast("double")) \
            .withColumn("Restaurant_latitude", trim(col("Restaurant_latitude")).cast("double")) \
            .withColumn("Restaurant_longitude", trim(col("Restaurant_longitude")).cast("double")) \
            .withColumn("Vehicle_condition", trim(col("Vehicle_condition")).cast("integer")) \
            .withColumn("multiple_deliveries", trim(col("multiple_deliveries")).cast("integer")) \
            .withColumn("Type_of_order", trim(col("Type_of_order"))) \
            .withColumn("Order_Date", to_date(trim(col("Order_Date")), "yyyy-MM-dd")) \
            .withColumn("Time_Orderd", to_timestamp(concat_ws(" ", trim(col("Order_Date")), trim(col("Time_Orderd"))), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("Time_Order_picked", to_timestamp(concat_ws(" ", trim(col("Order_Date")), trim(col("Time_Order_picked"))), "yyyy-MM-dd HH:mm:ss"))

        # Maybe here can wrap with a function
        print(parsed_df.printSchema())
        print("Starting Stream...")
        
        # Format change to "console" to debug 
        query = parsed_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", self.hdfs_output_path) \
            .option("checkpointLocation", self.hdfs_checkpoint_path) \
            .start()


        query.awaitTermination()

if __name__ == "__main__":
    consumer = SparkKafkaConsumer(topic = "testing")
    consumer.streams_subscription()
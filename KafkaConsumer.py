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
        self.hdfs_success_path = "hdfs://localhost:9000/user/student/delivery_data/success/"
        self.hdfs_late_path = "hdfs://localhost:9000/user/student/delivery_data/late/"
        self.hdfs_checkpoint_path = "hdfs://localhost:9000/user/student/delivery_data/checkpoint/"

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
            StructField("Weatherconditions", StringType(), True),
            StructField("Road_traffic_density", StringType(), True),
            StructField("Vehicle_condition", StringType(), True),
            StructField("Type_of_order", StringType(), True),
            StructField("Type_of_vehicle", StringType(), True),
            StructField("multiple_deliveries", StringType(), True),
            StructField("Festival", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Order_DateTime", StringType(), True),
            StructField("Picked_Datetime", StringType(), True),
            StructField("Delivered_Datetime", StringType(), True),
            StructField("Delivery_Time_Elapsed", StringType(), True)
        ])

        # .option("startingOffsets", "earliest") \ add it back future
        streams_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_server) \
            .option("subscribe", self.topic) \
            .load()

        parsed_df = streams_df.select(
            col("topic").alias("kafka_topic"), 
            from_json(col("value").cast("string"), schemas).alias("data")
        ).select("kafka_topic", "data.*")

        parsed_df = parsed_df \
            .withColumn("Delivery_person_ID", trim(col("Delivery_person_ID"))) \
            .withColumn("Delivery_person_Age", trim(col("Delivery_person_Age")).cast("integer")) \
            .withColumn("Delivery_person_Ratings", trim(col("Delivery_person_Ratings")).cast("double")) \
            .withColumn("Restaurant_latitude", trim(col("Restaurant_latitude")).cast("double")) \
            .withColumn("Restaurant_longitude", trim(col("Restaurant_longitude")).cast("double")) \
            .withColumn("Vehicle_condition", trim(col("Vehicle_condition")).cast("integer")) \
            .withColumn("multiple_deliveries", trim(col("multiple_deliveries")).cast("integer")) \
            .withColumn("Type_of_order", trim(col("Type_of_order"))) \
            .withColumn("Order_DateTime", to_timestamp(col("Order_DateTime"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("Picked_Datetime", to_timestamp(col("Picked_Datetime"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("Delivered_Datetime", to_timestamp(col("Delivered_Datetime"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("Delivery_Time_Elapsed", col("Delivery_Time_Elapsed").cast("double"))

        non_critical_string_cols = [
            "Weatherconditions", 
            "Road_traffic_density", 
            "Vehicle_condition", 
            "Type_of_order", 
            "Type_of_vehicle", 
            "Festival", 
            "City"
        ]
        parsed_df = parsed_df.fillna("Unknown", subset=non_critical_string_cols)


        def route_data_to_storage(batch_df, batch_id):
            batch_df.persist() 
            late_df = batch_df.filter(col("kafka_topic") == "Late_Delivery")
            if not late_df.isEmpty():
                print(f"Batch {batch_id}: Found LATE deliveries!")
                late_df.show(3)
                late_df.drop("kafka_topic").write.mode("append").format("parquet").save(self.hdfs_late_path)

            success_df = batch_df.filter(col("kafka_topic") == "Delivered_Succeed")
            if not success_df.isEmpty():
                print(f"Batch {batch_id}: Found SUCCESSFUL deliveries!")
                success_df.show(3)
                success_df.drop("kafka_topic").write.mode("append").format("parquet").save(self.hdfs_success_path)

            batch_df.unpersist()

            
        parsed_df.printSchema()
        print("Starting Stream...")

        '''
        Format change to "console" to debug
        .option("path", self.hdfs_output_path) \
        '''
         
        query = parsed_df.writeStream \
            .outputMode("append") \
            .foreachBatch(route_data_to_storage) \
            .option("checkpointLocation", self.hdfs_checkpoint_path) \
            .start()

        query.awaitTermination()

if __name__ == "__main__":
    consumer = SparkKafkaConsumer(topic = "Late_Delivery,Delivered_Succeed")
    consumer.streams_subscription()
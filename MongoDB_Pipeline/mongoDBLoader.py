from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class CuratedDataToMongoLoader:
    def __init__(self, connection):
        self.mongodb_uri = connection
        self.database_name = "food_delivery_db"
        
        self.curated_data_path = "hdfs://localhost:9000/user/student/delivery_data/curated_data/master_curated_data.parquet/"        
        self.spark = self.build_spark_session()

    def build_spark_session(self):
        return SparkSession.builder \
            .appName("HdfsToMongoNormalizedETL") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0") \
            .getOrCreate()

    def extract_transform_load(self):
        df = self.spark.read.parquet(self.curated_data_path)
        df = df.withColumn("Delivery_Status", F.when(F.col("Delivery_Time_Elapsed") > 30, "Late_Delivery").otherwise("Delivered_Succeed"))

        orders_df = df.select(
            F.col("ID").alias("_id"),
            F.col("Delivery_person_ID").alias("delivery_person_id"),
            F.col("City").alias("city"),
            F.col("Road_traffic_density").alias("road_traffic_density"),
            F.col("Type_of_order").alias("type_of_order"),
            F.col("Type_of_vehicle").alias("type_of_vehicle"),
            F.col("Delivery_Time_Elapsed").cast("float").alias("delivery_time_elapsed"),
            F.col("Delivery_person_Ratings").cast("float").alias("rating"),
            F.col("Delivery_Status").alias("delivery_status")
        )
        
        orders_df.write.format("mongodb") \
            .option("connection.uri", self.mongodb_uri) \
            .option("database", self.database_name) \
            .option("collection", "delivery_orders") \
            .mode("overwrite").save()
        
        self.spark.stop()
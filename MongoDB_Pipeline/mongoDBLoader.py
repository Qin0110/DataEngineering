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
        
        df = df.withColumn("picked_t", F.to_timestamp(F.trim(F.col("Picked_Datetime")), "yyyy-MM-dd HH:mm:ss"))
        df = df.withColumn("delivered_t", F.to_timestamp(F.trim(F.col("Delivered_Datetime")), "yyyy-MM-dd HH:mm:ss"))
        df = df.withColumn("Delivery_Time_Elapsed", F.round((F.unix_timestamp("delivered_t") - F.unix_timestamp("picked_t")) / 60, 2))        
        df = df.withColumn("deliverable", F.when(F.col("Delivery_Time_Elapsed") > 30, "Late_Delivery").otherwise("Delivered_Succeed"))
        
        df = df.withColumn("ID", F.trim(F.col("ID")))
        df = df.withColumn("Delivery_person_ID", F.trim(F.col("Delivery_person_ID")))
        df = df.withColumn("City", F.trim(F.col("City")))
        df = df.withColumn("Road_traffic_density", F.trim(F.col("Road_traffic_density")))
        df = df.withColumn("Type_of_order", F.trim(F.col("Type_of_order")))
        df = df.withColumn("Type_of_vehicle", F.trim(F.col("Type_of_vehicle")))
        df = df.withColumn("Weatherconditions", F.trim(F.regexp_replace(F.col("Weatherconditions"), "^conditions\\s*", "")))
        df = df.withColumn("Festival", F.trim(F.col("Festival")))
        df = df.withColumn("Delivery_person_Ratings", F.col("Delivery_person_Ratings").cast("float"))
        df = df.withColumn("Delivery_person_Age", F.col("Delivery_person_Age").cast("int"))

        cleaned_df = df.filter(
            (F.col("ID").isNotNull()) &
            (F.col("Delivery_Time_Elapsed").isNotNull()) &
            (F.col("City") != "Unknown") &
            (~F.col("Delivery_person_Ratings").isin(["NaN", "nan", ""])) 
        )

        orders_df = cleaned_df.select(
            F.col("ID").alias("_id"),
            F.col("Delivery_person_ID").alias("delivery_person_id"),
            F.col("City").alias("city"),
            F.col("Road_traffic_density").alias("road_traffic_density"),
            F.col("Type_of_order").alias("type_of_order"),
            F.col("Type_of_vehicle").alias("type_of_vehicle"),
            F.col("deliverable"),            
            F.col("Delivery_Time_Elapsed").cast("float").alias("delivery_time_elapsed"),
            F.col("Delivery_person_Ratings").cast("float").alias("rating")
        )
        
        orders_df.write.format("mongodb") \
            .option("spark.mongodb.connection.uri", self.mongodb_uri) \
            .option("spark.mongodb.database", self.database_name) \
            .option("spark.mongodb.collection", "delivery_orders") \
            .mode("overwrite").save()
        
        self.spark.stop()
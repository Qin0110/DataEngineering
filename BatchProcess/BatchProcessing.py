from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when, avg, round, upper
from BatchReport import DeliveryReportGenerator

class BatchProcessing:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Assignment").getOrCreate()
        self.categorical_columns = [
            "Weatherconditions",
            "Road_traffic_density",
            "Type_of_order",
            "Type_of_vehicle",
            "Festival",
            "City"
        ]
        self.identifier_columns = [
            "ID",
            "Delivery_person_ID"
        ]
        self.datetime_columns = [
            "Order_DateTime",
            "Picked_Datetime",
            "Delivered_Datetime"
        ]
        self.numerical_columns = [
            "Delivery_person_Age",
            "Delivery_person_Ratings",
            "Restaurant_latitude", "Restaurant_longitude",
            "Delivery_location_latitude", "Delivery_location_longitude"
        ]
        self.ordinal_columns = [
            "Vehicle_condition",
            "multiple_deliveries"
        ]
        self.hdfs_path = "hdfs://localhost:9000/user/student/"
        self.historcal_data_path = "assignment_data/historical_data.csv"
        self.streams_late_input = "delivery_data/late/"
        self.streams_success_input = "delivery_data/success/"
        self.output_path = "delivery_data/curated_data/"

    def data_processing(self, spark_data_frame):
        spark_data_frame = spark_data_frame.dropDuplicates()

        for column in self.categorical_columns:
            spark_data_frame = spark_data_frame.withColumn(column, lower(trim(col(column))))

        spark_data_frame = spark_data_frame.withColumn("Delivery_person_ID", upper(trim(col("Delivery_person_ID"))))

        critical_cols = [
            "Delivery_person_ID",
            "Order_DateTime",
            "Picked_Datetime",
            "Delivered_Datetime",
        ]

        for c in critical_cols:
            spark_data_frame = spark_data_frame.withColumn(
                c,
                when(
                    col(c).isNull() |
                    (trim(col(c).cast("string")) == "") |
                    (lower(trim(col(c).cast("string"))).isin("null", "na", "n/a", "nan")),
                    None
                ).otherwise(col(c))
            )

        spark_data_frame = spark_data_frame.na.drop(subset=critical_cols)

        for column in self.categorical_columns:
            spark_data_frame = spark_data_frame.withColumn(
                column,
                when(
                    col(column).isNull() |
                    (trim(col(column)) == "") |
                    (lower(trim(col(column))).isin("null", "na", "n/a", "nan","condition nan")),
                    "unknown"
                ).otherwise(col(column))
            )

        for column in self.ordinal_columns:
            spark_data_frame = spark_data_frame.withColumn(
                column,
                when(
                    col(column).isNull() |
                    (trim(col(column).cast("string")) == "") |
                    (lower(trim(col(column).cast("string"))).isin("null", "na", "n/a", "nan")),
                    None
                ).otherwise(col(column)).cast("integer")            
            )
            
            q1, median_val, q3 = spark_data_frame.filter(col(column).isNotNull()).approxQuantile(column, [0.25, 0.5, 0.75], 0.01)
        
            spark_data_frame = spark_data_frame.withColumn(
                column,
                when(col(column).isNull(), median_val).otherwise(col(column))
            )

        mean_val = spark_data_frame.select(avg("Delivery_person_Age")).collect()[0][0]

        spark_data_frame = spark_data_frame.withColumn(
            "Delivery_person_Age",
            when(
                col("Delivery_person_Age").isNull() |
                (trim(col("Delivery_person_Age").cast("string")) == "") |
                (lower(trim(col("Delivery_person_Age").cast("string"))).isin("null", "na", "n/a", "nan")),
                mean_val
            ).otherwise(col("Delivery_person_Age"))
        )

        spark_data_frame = spark_data_frame.withColumn(
            "Delivery_person_Ratings",
            when(
                col("Delivery_person_Ratings").isNull() |
                (trim(col("Delivery_person_Ratings").cast("string")) == "") |
                (lower(trim(col("Delivery_person_Ratings").cast("string"))).isin("null", "na", "n/a", "nan")),
                None
            ).otherwise(col("Delivery_person_Ratings")).cast("double")
        )

        spark_data_frame = spark_data_frame.filter(
            (col("Vehicle_condition") >= 0) & (col("Vehicle_condition") <= 3) &
            (col("Delivery_person_Ratings") >= 0) & (col("Delivery_person_Ratings") <= 5) &
            (col("multiple_deliveries") >= 0)
        )
        
        spark_data_frame = spark_data_frame.filter(
            (col("Picked_Datetime") >= col("Order_DateTime")) &
            (col("Delivered_Datetime") >= col("Picked_Datetime"))
        )

        spark_data_frame = spark_data_frame.withColumn(
            "Delivery_Time_Elapsed",
            round((col("Delivered_Datetime").cast("long") - col("Picked_Datetime").cast("long")) / 60, 2)
        )

        spark_data_frame = spark_data_frame.withColumn(
            "Pickup_Delay_Minutes",
            round((col("Picked_Datetime").cast("long") - col("Order_DateTime").cast("long")) / 60 , 2)
        )

        spark_data_frame = spark_data_frame.filter(
            (col("Restaurant_latitude") != 0) & (col("Restaurant_latitude") >= -90) & (col("Restaurant_latitude") <= 90) &
            (col("Restaurant_longitude") != 0) & (col("Restaurant_longitude") >= -180) & (col("Restaurant_longitude") <= 180) &
            (col("Delivery_location_latitude") != 0) & (col("Delivery_location_latitude") >= -90) & (col("Delivery_location_latitude") <= 90) &
            (col("Delivery_location_longitude") != 0) & (col("Delivery_location_longitude") >= -180) & (col("Delivery_location_longitude") <= 180)
        )

        return spark_data_frame
            

    def historical_data(self):
        df = self.spark.read.csv(self.hdfs_path + self.historcal_data_path, header = True, inferSchema = True)
        return df

    def handle_late_data(self):
        df = self.spark.read.parquet(self.hdfs_path + self.streams_late_input)
        return df

    def handle_suceed_data(self):
        df = self.spark.read.parquet(self.hdfs_path + self.streams_success_input)
        return df
        
    def store_curated_data(self, spark_data_frame, filename):
        spark_data_frame.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(self.hdfs_path + self.output_path + filename)

if __name__ == "__main__":
    batch_process = BatchProcessing()

    raw_historical = batch_process.historical_data()
    raw_late = batch_process.handle_late_data()
    raw_success = batch_process.handle_suceed_data()
    
    unified_df = raw_historical \
        .unionByName(raw_late, allowMissingColumns=True) \
        .unionByName(raw_success, allowMissingColumns=True)

    clean_unified_df = batch_process.data_processing(unified_df)
    
    batch_process.store_curated_data(clean_unified_df, "master_curated_data.parquet")

    reporter = DeliveryReportGenerator(batch_process.spark)
    
    reporter.display_reports(clean_unified_df)

    reporter.display_late_delivery_report(clean_unified_df)
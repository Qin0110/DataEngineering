from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, avg, countDistinct, count, round, expr, when, lower, trim

class DeliveryReportGenerator:
    def __init__(self, spark_session):
        self.spark = spark_session

    def display_reports(self, df):
        
        print("\n" + "="*50)
        print("     CALCULATING MASTER DELIVERY REPORT...    ")
        print("="*50 + "\n")
        
        df = df.withColumn(
            "order_date",
            to_date(col("Order_DateTime"))
        ).withColumn(
            "order_month", 
            date_format("Order_DateTime", "yyyy-MM")
        )

        df.cache()

        print("--- 1. OVERALL KPI SUMMARY ---")
        df.agg(
            expr("count(*)").alias("total_orders"),
            round(avg("Delivery_Time_Elapsed"), 2).alias("avg_delivery_minutes"),
            round(avg("Pickup_Delay_Minutes"), 2).alias("avg_pickup_delay_minutes"),
            round(avg("Delivery_person_Ratings"), 2).alias("avg_rider_rating"),
            countDistinct("Delivery_person_ID").alias("distinct_delivery_persons")
        ).show(truncate=False)

        print("--- 2. CITY ANALYSIS ---")
        df.groupBy("City").agg(
            count("*").alias("total_orders"),
            round(avg("Delivery_Time_Elapsed"), 2).alias("avg_delivery_minutes"),
            round(avg("Pickup_Delay_Minutes"), 2).alias("avg_pickup_delay_minutes"),
            round(avg("Delivery_person_Ratings"), 2).alias("avg_rider_rating"),
        ).orderBy("total_orders", ascending=False).show(truncate=False)

        rider_analysis = df.groupBy("Delivery_person_ID").agg(
            count("*").alias("total_orders"),
            round(avg("Delivery_Time_Elapsed"), 2).alias("avg_delivery_minutes"),
            round(avg("Pickup_Delay_Minutes"), 2).alias("avg_pickup_delay_minutes"),
            round(avg("Delivery_person_Ratings"), 2).alias("avg_rider_rating")
        )
        
        print("--- 3. TOP 10 BEST PERFORMING RIDERS ---")
        rider_analysis.orderBy("avg_rider_rating", ascending=False).show(10, truncate=False)

        print("--- 4. BOTTOM 10 LOWEST PERFORMING RIDERS ---")
        rider_analysis.filter(col("avg_rider_rating").isNotNull()) \
                      .orderBy("avg_rider_rating", ascending=True) \
                      .show(10, truncate=False)

        print("--- 5. DAILY TRENDS ---")
        df.groupBy("order_date").agg(
            count("*").alias("total_orders"),
            round(avg("Delivery_Time_Elapsed"), 2).alias("avg_delivery_minutes"),
            round(avg("Pickup_Delay_Minutes"), 2).alias("avg_pickup_delay_minutes")
        ).orderBy("order_date").show(truncate=False)

        print("--- 6. MONTHLY TRENDS ---")
        df.groupBy("order_month").agg(
            count("*").alias("total_orders"),
            round(avg("Delivery_Time_Elapsed"), 2).alias("avg_delivery_minutes"),
            round(avg("Pickup_Delay_Minutes"), 2).alias("avg_pickup_delay_minutes"),
            round(avg("Delivery_person_Ratings"), 2).alias("avg_rider_rating")
        ).orderBy("order_month").show(truncate=False)

        print("====================================================")
        print("                 END OF REPORT                      ")
        print("====================================================\n")


    def display_late_delivery_report(self, df):
        print("\n" + "!"*50)
        print("      🚨 LATE DELIVERY DEEP-DIVE REPORT 🚨      ")
        print("!"*50 + "\n")
        
        df = df.withColumn(
            "order_date",
            to_date(col("Order_DateTime"))
        )

        late_df = df.filter(col("Delivery_Time_Elapsed") > 30)

        late_df.cache()

        total_late = late_df.count()
        if total_late == 0:
            print("✅ Great news! There are ZERO late deliveries in this dataset.")
            print("!"*50 + "\n")
            return

        print(f"--- 1. OVERALL LATE KPIs (Total Late Orders: {total_late}) ---")
        late_df.agg(
            round(avg("Delivery_Time_Elapsed"), 2).alias("avg_late_delivery_time"),
            round(avg(col("Delivery_Time_Elapsed") - 30), 2).alias("avg_minutes_past_deadline"),
            round(avg("Pickup_Delay_Minutes"), 2).alias("avg_restaurant_wait_time"),
            round(avg("Delivery_person_Ratings"), 2).alias("avg_rating_on_late_orders")
        ).show(truncate=False)

        print("--- 2. WORST CITIES FOR LATE DELIVERIES ---")
        late_df.groupBy("City").agg(
            count("*").alias("total_late_orders"),
            round(avg("Delivery_Time_Elapsed"), 2).alias("avg_delivery_time")
        ).orderBy("total_late_orders", ascending=False).show(truncate=False)

        print("--- 3. TOP 10 RIDERS WITH MOST LATE DELIVERIES ---")
        late_df.groupBy("Delivery_person_ID").agg(
            count("*").alias("late_offenses"),
            round(avg("Delivery_Time_Elapsed"), 2).alias("avg_time_when_late"),
            round(avg("Delivery_person_Ratings"), 2).alias("rider_rating")
        ).orderBy("late_offenses", ascending=False).show(10, truncate=False)

        print("--- 4. DAILY LATE DELIVERY TREND ---")
        late_df.groupBy("order_date").agg(
            count("*").alias("late_orders_that_day"),
            round(avg("Delivery_Time_Elapsed"), 2).alias("avg_late_time_that_day")
        ).orderBy("order_date").show(truncate=False)

        print("====================================================")
        print("              END OF LATE REPORT                    ")
        print("====================================================\n")
import pandas as pd
import numpy as np
import os

class DataPreparation:
    def __init__(self):
        self.train_set = 'train.csv'
        self.test_set = 'test.csv'
        self.output_csv = 'dataset.csv'

    def combine_dataset(self):
        df_train = pd.read_csv(self.train_set)
        df_test = pd.read_csv(self.test_set)

        df_train = df_train.drop("Time_taken(min)", axis = 1)
        
        full_dataset = pd.concat([df_train, df_test], ignore_index = True)
        full_dataset['Order_Date'] = pd.to_datetime(full_dataset['Order_Date'])    
        full_dataset['Time_Orderd'] = pd.to_datetime(full_dataset['Time_Orderd'].str.strip(), errors='coerce').dt.strftime('%H:%M:%S')
        full_dataset = full_dataset.sort_values(
            by=['Order_Date', 'Time_Orderd'], 
            ascending=[True, True], 
            na_position='last' 
        )

        full_dataset = self.combine_date_time(full_dataset)
        full_dataset = self.synthetic_data(full_dataset)
        
        full_dataset.to_csv(self.output_csv, index = False)

    def combine_date_time(self, full_dataset):
        temp_date_order = full_dataset['Order_Date'].astype(str) + " " + full_dataset['Time_Orderd'].astype(str)
        full_dataset['Order_DateTime'] = pd.to_datetime(temp_date_order, format='%Y-%m-%d %H:%M:%S', errors = "coerce")
        
        temp_date_picked = full_dataset['Order_Date'].astype(str) + " " + full_dataset['Time_Order_picked'].astype(str)
        full_dataset['Picked_Datetime'] = pd.to_datetime(temp_date_picked, format='%Y-%m-%d %H:%M:%S', errors = "coerce")
        return full_dataset

    def synthetic_data(self, full_dataset):
        random_minutes = np.random.randint(15, 61, size=len(full_dataset))
        random_deltas = pd.to_timedelta(random_minutes, unit='m')
        
        full_dataset['Delivered_Datetime'] = full_dataset['Picked_Datetime'] + random_deltas
        full_dataset = full_dataset.drop(["Order_Date", "Time_Orderd", "Time_Order_picked"], axis=1)
        return full_dataset

if __name__ == "__main__":
    data = DataPreparation()
    data.combine_dataset()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, trim, to_timestamp, date_format

# class DataPreparation:
#     def __init__(self):
#         # Initialize SparkSession
#         self.spark = SparkSession.builder \
#             .appName("DatasetPreparation") \
#             .getOrCreate()
            
#         self.train_set = 'assignment-data/train.csv'
#         self.test_set = 'assignment-data/test.csv'
#         self.output_dir = 'assignment-data/dataset_output' # Spark writes to a directory

#     def combine_dataset(self):
#         # Read datasets
#         df_train = self.spark.read.csv(self.train_set, header=True, inferSchema=True)
#         df_test = self.spark.read.csv(self.test_set, header=True, inferSchema=True)

#         # Combine datasets (unionByName aligns by column name, similar to pd.concat)
#         full_dataset = df_train.unionByName(df_test, allowMissingColumns=True)

#         # Convert Order_Date to timestamp
#         full_dataset = full_dataset.withColumn(
#             'Order_Date', 
#             to_timestamp(col('Order_Date'))
#         )

#         # Clean, coerce errors, and format Time_Orderd
#         full_dataset = full_dataset.withColumn(
#             'Time_Orderd',
#             # 1. trim() removes whitespace (like .str.strip())
#             # 2. to_timestamp() converts to time. If it fails, it naturally returns null (like errors='coerce')
#             # 3. date_format() extracts and formats the string to HH:mm:ss (like .dt.strftime())
#             date_format(to_timestamp(trim(col('Time_Orderd'))), 'HH:mm:ss')
#         )

#         # Sort values with nulls last
#         full_dataset = full_dataset.orderBy(
#             col('Order_Date').asc(),
#             col('Time_Orderd').asc_nulls_last()
#         )

#         # Write to CSV (coalesce(1) outputs a single file)
#         full_dataset.coalesce(1).write.csv(self.output_dir, header=True, mode='overwrite')
        
#         print(f"Data successfully processed and saved to {self.output_dir}/")
        
#         # Stop session
#         self.spark.stop()

# if __name__ == "__main__":
#     data = DataPreparation()
#     data.combine_dataset()
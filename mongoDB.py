from MongoDB_Pipeline.mongoDBConnection import PyMongoUtils, CONNECTION_STRING
from MongoDB_Pipeline.mongoDBLoader import CuratedDataToMongoLoader
from MongoDB_Pipeline.mongoDBQuery import DeliveryMetricsQueryService
import pandas as pd
import os

def run_pipeline():
    print("=========================================")
    print(" Phase 1: Connection + Load Data")
    print("=========================================")
    loader = CuratedDataToMongoLoader(CONNECTION_STRING)
    loader.extract_transform_load()

    print("\n=========================================")
    print(" Phase 2: MongoDB Query ")
    print("=========================================")
    db_utils = PyMongoUtils(CONNECTION_STRING)
    db = db_utils.get_database("food_delivery_db")
    query_service = DeliveryMetricsQueryService(db)

    print("\nQUERY 1: Vehicle Success Rate")
    vehicle_ratings = query_service.get_vehicle_rating()
    
    if not vehicle_ratings:
        print("No vehicle success rate.")
    else:
        print("-" * 118)
        print(f"| {'Vehicle':<20} | {'Volume':<8} | {'Avg Time (M)':<12} |{'Avg Rating':<12} | {'No. Success':<14} | {'No. Late':<14} | {'Success rate (%)':<17} |")
        print("-" * 118)
        for record in vehicle_ratings:
            print(
                f"| {str(record.get('Vehicle')):<20} | "
                f"{str(record.get('Volume', 0)):<8} | "
                  f"{str(record.get('Avg_Delivery_Time')):<12} |"
                f"{str(record.get('Avg_Rating')):<12} |"
                f"{str(record.get('Success_Orders', 0)):<15} | "
                f"{str(record.get('Late_Orders')):<14} | "
                f"{record.get('Success_Rate_Percent'):<17} | "
            )
        print("-" * 118)
        save_to_csv(vehicle_ratings, "q1_vehicle_success_rate.csv")

    print("\nQUERY 2: City Food Performance Bottlenecks")
    performance_report = query_service.get_city_food_performance()
    
    if not performance_report:
        print("No performance data found.")
    else:
        print("-" * 78)
        print(f"| {'City':<15} | {'Food Type':<12} | {'Volume':<8} | {'Late Rate (%)':<15} | {'Avg Time (M)':<12} |")
        print("-" * 78)
        for record in performance_report:
            print(
                f"| {str(record.get('City')):<15} | "
                f"{str(record.get('FoodType')):<12} | "
                f"{str(record.get('Volume')):<8} | "
                f"{str(record.get('Late_Rate_Percent', 0)):<15} | "
                f"{str(record.get('Avg_Delivery_Time', 0)):<12} |"
            )
        print("-" * 78)
        save_to_csv(performance_report, "q2_city_food_performance.csv")

def save_to_csv(data, filename):
    if not data:
        print(f"No data to save for {filename}")
        return
    
    df = pd.DataFrame(data)
    
    if "Vehicle" in df.columns:
        desired_order = [
            "Vehicle", 
            "Volume", 
            "Avg_Delivery_Time", 
            "Avg_Rating", 
            "Success_Orders", 
            "Late_Orders", 
            "Success_Rate_Percent"
        ]
        df = df[desired_order]
        
    elif "City" in df.columns:
        desired_order = [
            "City", 
            "FoodType", 
            "Volume", 
            "Late_Rate_Percent", 
            "Avg_Delivery_Time"
        ]
        df = df[desired_order]

    os.makedirs("mongoDB_Pipeline", exist_ok =True)
    filepath = os.path.join("MongoDB_Pipeline", filename)
    
    df.to_csv(filepath, index=False)
    
    print(f"Saved {len(df)} records to {filepath}")

if __name__ == "__main__":
    run_pipeline()
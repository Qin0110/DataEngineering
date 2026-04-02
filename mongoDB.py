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
                f"| {str(record.get('Vehicle', 'N/A')):<20} | "
                f"{str(record.get('Volume', 0)):<8} | "
                  f"{str(record.get('Avg_Delivery_Time')):<12} |"
                f"{str(record.get('Avg_Rating')):<12} |"
                f"{str(record.get('Success_Orders', 0)):<15} | "
                f"{str(record.get('Late_Orders')):<14} | "
                f"{record.get('Success_Rate_Percent'):<17} | "
            )
        print("-" * 118)
        save_to_csv(vehicle_ratings, "q1_vehicle_success_rate.csv")

    print("\nQUERY 2: Food Demand by City")
    food_report = query_service.get_food_demand_by_city()
    if not food_report:
        print("No food data found.")
    else:
        print("-" * 55)
        print(f"| {'City':<15} | {'Food Type':<15} | {'Volume':<15} |")
        print("-" * 55)
        for record in food_report:
            print(
                f"| {str(record.get('City')):<15} | "
                f"{str(record.get('FoodType')):<15} | "
                f"{str(record.get('Volume')):<15} |"
            )
        print("-" * 55)
        save_to_csv(food_report, "q2_food_demand_by_city.csv")

def save_to_csv(data, filename):
    if not data:
        print(f"No data to save for {filename}")
        return
    
    df = pd.DataFrame(data)
    
    df.to_csv(filename, index=False)
    
    print(f"Saved {len(df)} records to {filename}")

if __name__ == "__main__":
    run_pipeline()
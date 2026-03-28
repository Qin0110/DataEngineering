from MongoDB.mongoDBConnection import PyMongoUtils, CONNECTION_STRING
from MongoDB.mongoDBLoader import CuratedDataToMongoLoader
from MongoDB.mongoDBQuery import DeliveryMetricsQueryService

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

    print("\n--- Query 1: Fleet Performance by Vehicle ---")
    late_orders = query_service.get_vehicle_rating()
    if not late_orders:
        print("No late data found.")
    else:
        print("-" * 80)
        print(f"| {'Vehicle':<20} | {'Volume':<10} | {'Late Orders':<15} | {'Success Orders':<15} | {'Avg Rating':<10} |")
        print("-" * 80)
        for record in late_orders:
            print(
                f"| {str(record.get('Vehicle', 'N/A')):<20} | "
                f"{str(record.get('Volume', 0)):<10} | "
                f"{str(record.get('Late_Orders', 0)):<15} | "
                f"{str(record.get('Success_Orders', 0)):<15} | "
            )
        print("-" * 80)

    print("\n--- QUERY 2: Food Demand by City ---")
    food_report = query_service.get_food_demand_by_city()
    if not food_report:
        print("No food data found.")
    else:
        print("-" * 55)
        print(f"| {'City':<15} | {'Food Type':<15} | {'Volume':<15} |")
        print("-" * 55)
        for record in food_report:
            print(
                f"| {str(record.get('City', 'N/A')):<15} | "
                f"{str(record.get('FoodType', 'N/A')):<15} | "
                f"{str(record.get('Volume', 0)):<15} |"
            )
        print("-" * 55)

if __name__ == "__main__":
    run_pipeline()
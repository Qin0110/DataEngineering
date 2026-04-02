class DeliveryMetricsQueryService:
    def __init__(self, database):
        self.db = database

    def get_vehicle_rating(self):
        pipeline = [
            {"$match": {
                "delivery_time_elapsed": {"$ne": None}, 
                "rating": {"$nin": ["NaN", "nan", "", None]}
            }},
            {"$group": {
                    "_id": {"vehicle": "$type_of_vehicle"},
                    "Late_Orders": {"$sum": {"$cond": [{"$eq": ["$delivery_status", "Late_Delivery"]}, 1, 0]}},
                    "Success_Orders": {"$sum": {"$cond": [{"$eq": ["$delivery_status", "Delivered_Succeed"]}, 1, 0]}},
                    "Volume": {"$sum": 1},
                    "Avg_Delivery_Time": {"$avg": "$delivery_time_elapsed"},
                    "Avg_Rating":        {"$avg": "$rating"}
                }
            },
            {"$project": {
                    "_id": 0,
                    "Vehicle": "$_id.vehicle",
                    "Volume": 1,
                    "Avg_Delivery_Time": {"$round": ["$Avg_Delivery_Time", 1]},
                    "Avg_Rating": {"$round": ["$Avg_Rating",2]},
                    "Late_Orders": 1,
                    "Success_Orders": 1,
                    "Volume": 1,
                    "Success_Rate_Percent": {"$round": [{"$multiply": [{"$divide": ["$Success_Orders", "$Volume"]}, 100]},2]}
                }
            },
            {"$sort": {"Success_Rate_Percent": -1}}
        ]
        return list(self.db["delivery_orders"].aggregate(pipeline))

    def get_city_food_performance(self):
        pipeline = [
            {"$match": {
                "city": {"$nin": ["Unknown", "unknown", "NaN", "nan", "", None]},
                "delivery_time_elapsed": {"$ne": None} 
            }},
            {"$group": {
                "_id": {
                    "city": "$city",
                    "food_type": "$type_of_order"
                },
                "Volume": {"$sum": 1},
                "Late_Orders": {"$sum": {"$cond": [{"$eq": ["$delivery_status", "Late_Delivery"]}, 1, 0]}},
                "Success_Orders": {"$sum": {"$cond": [{"$eq": ["$delivery_status", "Delivered_Succeed"]}, 1, 0]}},
                "Avg_Delivery_Time": {"$avg": "$delivery_time_elapsed"}
            }},
            {"$project": {
                "_id": 0,
                "City": "$_id.city",
                "FoodType": "$_id.food_type",
                "Volume": 1,
                "Late_Orders": 1,
                "Success_Orders": 1,
                "Late_Rate_Percent": {"$round": [{"$multiply": [{"$divide": ["$Late_Orders", "$Volume"]}, 100]}, 2]},
                "Avg_Delivery_Time": {"$round": ["$Avg_Delivery_Time", 1]}
            }},
            {"$sort": {"City": 1, "Late_Rate_Percent": -1}}
        ]
        return list(self.db["delivery_orders"].aggregate(pipeline))

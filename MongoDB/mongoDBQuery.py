class DeliveryMetricsQueryService:
    def __init__(self, database):
        self.db = database

    def get_vehicle_rating(self):
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "vehicle": "$type_of_vehicle",
                    },
                    "Late_Orders": {
                        "$sum": {
                            "$cond": [{"$eq": ["$kafka_topic", "Late_Delivery"]}, 1, 0]
                        }
                    },
                    "Success_Orders": {
                        "$sum": {
                            "$cond": [{"$eq": ["$kafka_topic", "Delivered_Succeed"]}, 1, 0]
                        }
                    },
                    "Volume": {"$sum": 1},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Vehicle": "$_id.vehicle",
                    "Late_Orders": 1,
                    "Success_Orders": 1,
                    "Volume": 1,
                }
            },
            {
                "$sort": {"Late_Orders": -1}
            }
        ]
    
        return list(self.db["delivery_conditions"].aggregate(pipeline))

    def get_food_demand_by_city(self):
        pipeline = [
            {"$group": {
                "_id": {"city": "$city",
                        "food_type": "$type_of_order"},
                "Volume": {"$sum": 1}}
            },
            {"$project": {
                "_id": 0,
                "City": "$_id.city",
                "FoodType": "$_id.food_type",
                "Volume": 1                                        
            }},
            {"$sort": {"City": 1, "Volume": -1}}
        ]
        return list(self.db["delivery_conditions"].aggregate(pipeline))
from pyspark.sql import SparkSession
from neo4j_pipeline.neo4jConnection import get_db_connection, NEO4J_USERNAME

def run_loader():
    spark = SparkSession.builder \
        .appName("Neo4j Integration") \
        .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    
    df_combined = spark.read.parquet("hdfs://localhost:9000/user/student/delivery_data/curated_data/master_curated_data.parquet")
    
    delivery_data = [row.asDict() for row in df_combined.collect()]
    
    batch_data = []
    for row in delivery_data:
        batch_data.append({
            "person_id": row["Delivery_person_ID"],
            "age": row["Delivery_person_Age"],
            "rating": row["Delivery_person_Ratings"],
            "order_id": row["ID"],
            "multiple": row["multiple_deliveries"],
            "is_festival": row["Festival"],
            "date": str(row["Order_DateTime"]), 
            "weather": row["Weatherconditions"],
            "traffic": row["Road_traffic_density"],
            "time_elapsed": row["Delivery_Time_Elapsed"], 
            "status": "Late_delivery" if row["Delivery_Time_Elapsed"] is not None and row["Delivery_Time_Elapsed"] > 30 else "Delivered_succeed",
            "order_type": row["Type_of_order"],
            "rest_lat": row["Restaurant_latitude"],
            "rest_long": row["Restaurant_longitude"],
            "vehicle_type": row["Type_of_vehicle"],
            "vehicle_condition": row["Vehicle_condition"],
            "city": row["City"]
        })
    
    
    chunk_size = 5000
    chunks = [batch_data[i:i + chunk_size] for i in range(0, len(batch_data), chunk_size)]
    
    
    print(f"Connecting to Neo4j. Sending data in {len(chunks)} batches...")
    
    cypher_query = """
        UNWIND $batch AS data
        
        MERGE (d:DeliveryPerson {id: data.person_id})
        SET d.age = data.age
        
        MERGE (o:Order {id: data.order_id})
        SET o.date = data.date, 
            o.weather = data.weather,
            o.traffic = data.traffic,
            o.time_elapsed = data.time_elapsed,
            o.status = data.status,
            o.multiple_deliveries = data.multiple,
            o.festival = data.is_festival,
            o.type = data.order_type
            
        MERGE (v:Vehicle {type: data.vehicle_type, condition: data.vehicle_condition})
        MERGE (c:City {name: data.city})

        WITH d, o, v, c, data, toString(data.rest_lat) + "," + toString(data.rest_long) AS location_id
        MERGE (r:Restaurant {id: location_id})
        SET r.latitude = data.rest_lat, r.longitude = data.rest_long
        
        // RELATIONSHIPS
        MERGE (d)-[rel:DELIVERED]->(o)
        SET rel.customer_rating = data.rating
        
        MERGE (o)-[:DELIVERED_USING]->(v) 
        MERGE (o)-[:DELIVERED_IN]->(c)
        MERGE (o)-[:PICKED_UP_FROM]->(r)
    """
    index_queries = [
        "CREATE INDEX FOR (d:DeliveryPerson) ON (d.id);",
        "CREATE INDEX FOR (o:Order) ON (o.id);",
        "CREATE INDEX FOR (v:Vehicle) ON (v.type);",
        "CREATE INDEX FOR (c:City) ON (c.name);",
        "CREATE INDEX FOR (r:Restaurant) ON (r.id);"
    ]

    with get_db_connection() as driver:
        for q in index_queries:
            try:
                driver.execute_query(q, database_=NEO4J_USERNAME)
            except Exception:
                pass
                
        for i, chunk in enumerate(chunks):
            driver.execute_query(cypher_query, batch=chunk, database_=NEO4J_USERNAME)
            print(f"Batch {i + 1}/{len(chunks)} successfully uploaded!")
    
    print("Graph Built")

if __name__ == "__main__":
    run_loader()



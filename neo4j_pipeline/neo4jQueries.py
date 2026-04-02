import pandas as pd
from neo4j_pipeline.neo4jConnection import get_db_connection, NEO4J_USERNAME

def run_analytic_queries():
    print("Starting Neo4j Analytics...")
    query_1 = """
        MATCH (d:DeliveryPerson)-[rel:DELIVERED]->(:Order)
        WITH d, AVG(toFloat(rel.customer_rating)) AS OverallAverage
        
        MATCH (d)-[:DELIVERED]->(o:Order)
        WHERE toLower(o.weather) IN ['conditions stormy', 'conditions sandstorms']
        AND toLower(o.status) = 'delivered_succeed' 
        
        WITH d.id AS DriverID, 
             ROUND(OverallAverage, 1) AS CurrentRating, 
             COUNT(o) AS DifficultDeliveriesCompleted
             
        RETURN DriverID, CurrentRating, DifficultDeliveriesCompleted
        ORDER BY DifficultDeliveriesCompleted DESC, CurrentRating DESC
        LIMIT 10;
    """
    
    
    query_2 = """
        MATCH (o:Order)-[:PICKED_UP_FROM]->(r:Restaurant)
        WITH r.latitude AS RestaurantLat, 
             r.longitude AS RestaurantLong, 
             COUNT(o) AS TotalPickups, 
             SUM(CASE WHEN o.status = 'Late_delivery' THEN 1 ELSE 0 END) AS LatePickups,
             AVG(o.time_elapsed) AS AvgDeliveryMinutes
        WHERE TotalPickups > 30
        RETURN RestaurantLat, RestaurantLong, TotalPickups, LatePickups, 
               ROUND((TOFLOAT(LatePickups) / TotalPickups) * 100, 2) AS LatePickupPercentage,
               ROUND(AvgDeliveryMinutes, 2) AS AvgDeliveryMinutes
        ORDER BY LatePickupPercentage DESC
        LIMIT 20;


    """
    
    with get_db_connection() as driver:
        print("\n--------------------------------- Query 1: Reliable Driver Identifier --------------------------------")
        records_1, _, _ = driver.execute_query(query_1, database_=NEO4J_USERNAME)
        df1 = pd.DataFrame([r.data() for r in records_1])
        df1.index = df1.index + 1
        print(df1.head(10))
        df1.to_csv('neo4j_pipeline/q1_reliable_drivers.csv', index=False)
        print("-> Saved as 'q1_reliable_drivers.csv'")

        print("\n---------------------------------- Query 2: Restaurant Bottlenecks -----------------------------------")
        records_2, _, _ = driver.execute_query(query_2, database_=NEO4J_USERNAME)
        df2 = pd.DataFrame([r.data() for r in records_2])
        df2.index = df2.index + 1
        print(df2.head(10))
        df2.to_csv("neo4j_pipeline/q2_restaurant_bottlenecks.csv", index=False)
        print("-> Saved as 'q2_restaurant_bottlenecks.csv'")
        
    print("Database connection closed. Analytics complete!")


if __name__ == "__main__":
    run_analytic_queries()

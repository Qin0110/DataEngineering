from neo4j import GraphDatabase
from neo4j_pipeline.neo4jCredentials import *

def get_db_connection():
    """Authenticates and returns an active Neo4j driver."""
    auth_token = (NEO4J_USERNAME, NEO4J_PASSWORD)
    driver = GraphDatabase.driver(NEO4J_URI, auth=auth_token)
    return driver
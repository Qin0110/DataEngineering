from pymongo import MongoClient

CONNECTION_STRING = "mongodb+srv://lgw:lgwdbpass@cluster0.8fziuhf.mongodb.net/?appName=Cluster0"

class PyMongoUtils:
    def __init__(self, uri=CONNECTION_STRING):
        self.uri = uri    

    def get_database(self, database_name):
        client = MongoClient(self.uri)
        return client[database_name]
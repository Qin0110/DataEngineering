from kafka import KafkaConsumer
import os
import csv
import json

class Consumer:
    def __init__(self, topic = "", bootstrap_server = "localhost:9092"):
        self.topic = topic
        self.bootstrap_server = bootstrap_server

    def get_data(self):
        consumer = KafkaConsumer(
            self.topic, 
            bootstrap_servers = self.bootstrap_server,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Loads bytes back into dict
        )
        for data in consumer:
            print(data)
    
if __name__ == "__main__":
    data_getter = Consumer(topic="testing")
    data_getter.get_data()

# Can be more than one consumer
# Problem: Multiple consumer class cannot communicate with each other. (Consumer A get "Data A" and Consumer B won't be getting "Data A")
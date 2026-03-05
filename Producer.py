from kafka import KafkaProducer
import os
import csv
import time
import json 

class DataStreamer:
    def __init__(self, topic = "", bootstrap_server = "localhost:9092"):
        self.dataFolder = "data"
        self.dataset = ["test.csv", "train.csv"]
        self.topic = topic
        self.bootstrap_server = bootstrap_server
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
    def dataPreparation(self):
        count = 0
        for filename in self.dataset:
            with open(os.path.join(self.dataFolder, filename), mode="r", encoding="utf-8") as data_entry:
                read_csv = csv.DictReader(data_entry)
                
                for row in read_csv:
                    # print(row)
                    self.producer.send(self.topic, row)
                    time.sleep(0.5)
                    count += 1
                    print("Sent ", count)
        self.producer.flush()
        
if __name__ == "__main__":
    streamer = DataStreamer(topic="testing")
    streamer.dataPreparation()
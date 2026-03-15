from kafka import KafkaProducer
from datetime import datetime
import os
import csv
import time
import json 

class DataStreamer:
    def __init__(self, bootstrap_server = "localhost:9092"):
        self.dataFolder = "data"
        self.dataset = ["streaming_dataset.csv"]
        self.bootstrap_server = bootstrap_server
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
    def start_streaming(self):
        count = 0
        for filename in self.dataset:
            with open(os.path.join(self.dataFolder, filename), mode="r", encoding="utf-8") as data_entry:
                read_csv = csv.DictReader(data_entry)
                
                for row in read_csv:
                    if (count < 100): #Debug use
                        # print(row)

                        try:
                            picked_str = row['Picked_Datetime'].strip()
                            delivered_str = row['Delivered_Datetime'].strip()

                            picked_time = datetime.strptime(f"{picked_str}", "%Y-%m-%d %H:%M:%S")
                            delivered_time = datetime.strptime(f"{delivered_str}", "%Y-%m-%d %H:%M:%S")

                            time_elapsed = ((delivered_time - picked_time).total_seconds()) / 60
                            row['Delivery_Time_Elapsed'] = round(time_elapsed, 2)
                            
                            if time_elapsed > 30:
                                target_topic = "Late_Delivery"
                            else:
                                target_topic = "Delivered_Succeed"
                            self.producer.send(target_topic, row)

                        except Exception as e:
                            print(f"Skipping row {count} due to data format error: {e}")
                            
                        time.sleep(0.5)
                        count += 1
                        print(f"Sent row {count} to topic: {target_topic}")
                        
        self.producer.flush()
        
if __name__ == "__main__":
    streamer = DataStreamer()
    streamer.start_streaming()
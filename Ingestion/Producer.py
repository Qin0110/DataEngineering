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
                    if (count < 1000):
                        is_anomaly, anomaly_msg = self.anomaly_data_handling(row)
                        
                        if is_anomaly:
                            try:
                                row['Anomaly_Reason'] = anomaly_msg
                                target_topic = "Anomaly_Data"
                                self.producer.send(target_topic, row)
                                print(f"Sent row {count + 1} to topic: {target_topic}")
                                print(f"REASON: {anomaly_msg}")
                            except Exception as e:
                                print(f"Skipping row {count + 1} due to data format error: {e}")
                        
                        else:
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
                                print(f"Sent row {count + 1} to topic: {target_topic}")
    
                            except Exception as e:
                                print(f"Skipping row {count + 1} due to data format error: {e}")
                                
                        time.sleep(0.1)
                        count += 1
                        
        self.producer.flush()

    def anomaly_data_handling(self, row):
        critical_columns = [
            'Delivery_person_ID',
            'Restaurant_latitude',
            'Restaurant_longitude',
            'Delivery_location_latitude',
            'Delivery_location_longitude',
            'Order_DateTime'
        ]
        coordinate_columns = [
            'Restaurant_latitude',
            'Restaurant_longitude',
            'Delivery_location_latitude',
            'Delivery_location_longitude'
        ]
        
        missing_fields = []
        for column in critical_columns:
            value = str(row.get(column, "")).strip().upper()

            if not value or value in ["NAN", "NULL", "NONE", "NA"]:
                missing_fields.append(column)
                continue

            if column in coordinate_columns:
                try:
                    if float(value) == 0.0:
                        missing_fields.append(f"{column} (Invalid 0.0)")
                except ValueError:
                    missing_fields.append(f"{column} (Not a number)")

        if missing_fields:
            message = f"Missing critical data: {', '.join(missing_fields)}"
            return True, message
            
        return False, "Appropriate Data"
        
if __name__ == "__main__":
    streamer = DataStreamer()
    streamer.start_streaming()
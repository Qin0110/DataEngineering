## CHECK HDFS LOCATION
linux command: `hdfs getconf -confKey fs.defaultFS`

## Services startup
start-dfs.sh

start-yarn.sh

zookeeper-server-start.sh $KAFKA\_HOME/config/zookeeper.properties \&

kafka-server-start.sh $KAFKA\_HOME/config/server.properties \&


## Create topics
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Late\_Delivery

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Delivered\_Succeed

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Anomaly_Data


## Create virtual env
`pythom -m venv {filename}`

cd {filename}

## Install env
`pip install requirements.txt`

## Activate environemnt
`source {filename}/bin/activate`

## ***read parquet file from HDFS:***

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

### ***change hdfs location if different path*** 
hdfs_df = spark.read.parquet("hdfs://localhost:9000/user/student/delivery_data/curated_data/master_curated_data.parquet")


## RUN FILE (each run on different terminal simultaneously)
***Ingestion + Streaming + StreamProcess***
*Run consumer first, wait for the messages finished pop up, then run Producer.*
`python StreamProcess/KafkaConsumer.py` \n
`python StreamProcess/AnomalyDataConsumer.py` \n
`python Ingestion/Producer.py`

***BatchProcess***
`python BatchProcess/BatchProcessing.py`

***MongoDB**
`python mongoDB.py`

***Neo4j***
`python neo4jMain.py`
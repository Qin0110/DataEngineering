## CHECK HDFS LOCATION
linux command: `hdfs getconf -confKey fs.defaultFS`

start-dfs.sh

start-yarn.sh

zookeeper-server-start.sh $KAFKA\_HOME/config/zookeeper.properties \&

kafka-server-start.sh $KAFKA\_HOME/config/server.properties \&



kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Late\_Delivery

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Delivered\_Succeed

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Anomaly_Data



## ***read parquet file from HDFS:***

from pyspark.sql import SparkSession



spark = SparkSession.builder.appName("ReadParquet").getOrCreate()


hdfs_df = spark.read.parquet("hdfs://localhost:9000/user/student/delivery_data/curated_data/master_curated_data.parquet")

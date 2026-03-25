## CHECK HDFS LOCATION
linux command: `hdfs getconf -confKey fs.defaultFS`


zookeeper-server-start.sh $KAFKA\_HOME/config/zookeeper.properties \&

kafka-server-start.sh $KAFKA\_HOME/config/server.properties \&



kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Late\_Delivery

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Delivered\_Succeed





## ***read parquet file from HDFS:***

from pyspark.sql import SparkSession



spark = SparkSession.builder.appName("ReadParquet").getOrCreate()



\# Point Spark at the entire folder, not just one file!

\# Spark will automatically combine all the part-0000.parquet files inside it.



hdfs\_df\_success = spark.read.parquet("hdfs://localhost:9000/user/student/delivery\_data/success/")

hdfs\_df\_late = spark.read.parquet("hdfs://localhost:9000/user/student/delivery\_data/late/")





hdfs\_df\_success.show(5)

hdfs\_df\_success.printSchema()





hdfs\_df\_late.show(5)

hdfs\_df\_late.printSchema()


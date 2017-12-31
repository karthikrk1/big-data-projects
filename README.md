# big-data-projects
A repository for various Big Data code to learn the technologies within Big Data

# Map Reduce

Map Reduce is the data processing framework for Hadoop and is written in Java and contains of two major phases - Map and Reduce phase. The Map phase reads the input data as input splits and produces (K,V) Key-Value pairs as output. While the Reduce phase reads the Map's output and returns a value. The Reduce can be thought of an aggregation function where the final result is always a scalar. 

# Sqoop

Sqoop is a data transfer tool that helps us trasnfer data from RDBMS to Hadoop/HDFS or vice-versa. It is a command based tool and has a connection string to connect to various RDBMS systems.

Sqoop has support for both import and export. Import is when we import data from RDBMS into Hadoop while export moves data from Hadoop to RDBMS. The Sqoop import command has various options to work with different import goals.

# Spark

Spark is a fast data processing framework that can effectively replace MapReduce in Hadoop ecosystem. Spark works in-memory and has high speeds over MapReduce. 

Spark uses the concept of RDD (Resilient Distributed Datasets) that can be distributed to do efficient parallel processing and can be recreated if it is lost. This makes it resilient and distributed. 

# Flume
Flume is a tool that is used to scrap web logs for example and put them to HDFS. Main difference between Sqoop and Flume is that Sqoop is more suited towards RDBMS or in other words structured data, while Flume is more geared towards unstructured data. 

In the examples, there are some Flume example applications which deal with scraping some web logs using gen_logs and pushing them to HDFS, Spark and Kafka using the memory channel, with some customizations. 

# Kafka

Kafka is a publish-subscribe messaging system which works on the concept of topics. Once the data is written to topics, the data is stored as per the rentention policy and the subscribers can poll the data either alone or in groups called subscriber groups.

Though Kafka and Flume seem to share some similarity in their working, the main difference between them is Flume is less reliable and scalable. In order to send towards multiple sinks, there should be multiple channels, one per sink. And data is not reliable, an error can lead to loss of data, while Kafka is more advanced in scalability and reliability

The problem with Kafka though is that it needs messages to be written to in topics and this means applications should also write to Kafka apart from logs. This can be solved by using Flume to read from logs and pushing to Kafka where both complement each other and make use of the advantages of both.

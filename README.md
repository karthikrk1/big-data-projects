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

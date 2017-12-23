#!/usr/bin/env bash

# This script introduces two new switches
# -m is an alias for num-mappers and can be used interchangeably
# The --as-sequencefile switch is used to store the output in HDFS
# as a sequence file.
# Sqoop supports four types of file formats
# * text file
# * sequence file
# * parquet file
# * avro data file
sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_imports/db \
 -m 2 \
 --as-sequencefile
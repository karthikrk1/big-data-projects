#!/usr/bin/env bash

# The sqoop import-all is used to import all the tables in the database
# into HDFS. This is used to do a bulk import for the first time
# and then we can use sqoop import to do an incremental load
# Some notes:
# * warehouse-dir is important. Because this imports more than one table
#   so it cannot store in one directory.
# * It is good to use the autoreset-to-one-mapper since some table may
#   not have a primary key and it will fail if not used.
# * Cannot use filtering and transformation. Using --query, --where
# * Incremental imports is not possible 

sqoop import-all \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --autoreset-to-one-mapper
#!/usr/bin/env bash

# This sqoop import is for importing to Hive from RDBMS
# The hive-import switch tells us it is a Hive import
# The hive-database is the database which we need the table to be in.
# The hive-table is the table which will be created and the data will be
# populated.

sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --hive-import \
 --hive-database karthik_test \
 --hive-table table_name \
 --num-mappers 2
#!/usr/bin/env bash

# This is part-2 of incremental imports. Here there is the 'where' switch
# This helps to run the where clause on the table and get only those results
# Also this uses the --append switch to append to the table instead of
# throwing an exception saying that the directory exists.
# Note: Here we only import the data for Jan 2014.

sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --where "date like '2014-01%'" \
 --target-dir /user/karthik/inc_imports \
 -m 2 \
 --append
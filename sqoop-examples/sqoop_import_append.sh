#!/usr/bin/env bash

# This version of sqoop_import contains the --append switch
# This will append the results to the existing directory if present
# instead of throwing an exception that the directory already exists.
# This is useful when we do incremental loads where we already have
# some data and we need to append to it. 
sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --num-mappers 1 \
 --append


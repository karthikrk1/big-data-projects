#!/usr/bin/env bash

# This sqoop import introduces the Hive Overwrite switch. This is used to
# overwrite a table that is already available in the database.

sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --hive-import \
 --hive-database karthik_test \
 --hive-table table_name \
 --hive-overwrite \
 --num-mappers 2
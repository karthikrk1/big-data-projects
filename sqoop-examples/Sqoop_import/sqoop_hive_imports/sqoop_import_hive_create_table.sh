#!/usr/bin/env bash

# This sqoop import introduces the create-hive-table switch. This is used to
# create a table that in the database and import data into it.
# This is mutually exclusive with the hive-overwrite switch.

sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --hive-import \
 --hive-database karthik_test \
 --hive-table table_name \
 --create-hive-table \
 --num-mappers 2
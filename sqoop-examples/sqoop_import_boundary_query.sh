#!/usr/bin/env bash

# This sqoop import uses the boundary query
# This is used when we want to use a different boundary conditions
# Normally sqoop runs this query to determine the start and end of the
# primary key to make equal splits for the mappers
# We can use this switch to do boundary query only for a subset we need
# and not on the whole table

sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --boundary-query 'select min(id), max(id) from table_name where id>=100'
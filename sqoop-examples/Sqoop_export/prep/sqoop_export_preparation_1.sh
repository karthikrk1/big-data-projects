#!/usr/bin/env bash

# Preparing for SQOOP EXPORT - Part 1
# This is to import two tables into Hive from the RDBMS
# Let's assume that the two tables are "orders" which contains order information
# and the second is "order_items".

sqoop import \
    --connect jdbc:mysql://db.hostname.com:3306/db \
    --username user \
    -P \
    --table orders \
    --hive-import \
    --hive-database karthik_db \
    --hive-table orders \
    --num-mappers 2


sqoop import \
    --connect jdbc:mysql://db.hostname.com:3306/db \
    --username user \
    -P \
    --table order_items \
    --hive-import \
    --hive-database karthik_db \
    --hive-table orders \
    --num-mappers 2
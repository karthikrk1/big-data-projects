#!/usr/bin/env bash

# This introduces the --columns switch for Sqoop Export
# This is used to export only certain columns from the hive/hdfs into MySQL
# This is useful when some columns are NULLable
sqoop export \
    --connect jdbc:mysql://db.hostname.com:3306/db \
    --username user \
    -P \
    --export-dir /user/hive/warehouse/karthik_test/daily_revenue \
    --input-fields-terminated-by "\001" \
    --table column_mapping_demo \
    --columns order_date, revenue
#!/usr/bin/env bash

# This introduces the basic sqoop export. The export-dir is the HDFS/Hive dir
# from which we need to export to RDBMS
# The input-fields-terminated-by tells sqoop the default field terminator
# which is \001 in Hive
# The table is the MySQL table which we are going to populate

sqoop export \
    --connect jdbc:mysql://db.hostname.com:3306/db \
    --username user \
    -P \
    --export-dir /user/hive/warehouse/karthik_test/daily_revenue \
    --input-fileds-terminated-by "\001" \
    --table daily_revenue
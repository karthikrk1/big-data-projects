#!/usr/bin/env bash

# This sqoop import introduces three new switches
# null-non-string -> This switch is used to populate the HDFS data with
#                    some dummy value for a non-string column when it is NULL
# fields-terminated-by -> This switch is used to delimit the columns with
#                         a custom field delimiter
# lines-terminated-by -> This switch is useful to use a different character
#                        as a line delimiter.
sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --null-non-string -1 \
 --fields-terminated-by "\t" \
 --lines-terminated-by ":"
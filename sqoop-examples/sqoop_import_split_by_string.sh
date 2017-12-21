#!/usr/bin/env bash

# This query fails

:'
sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --split-by string_column_name
'

# The below query works
# The reason is by default splitting across a String column is disabled
# The -D control argument is needed to make the text splitter to true
# This then makes the split-by work on the text column.

sqoop import \
 -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --split-by string_column_name

#!/usr/bin/env bash

# This command introduces the --num-mappers switch. This is necessary because
# the num of mappers are chosen based on the input split which is in turn
# based on the primary key and how equally a table can be split to
# parallelly run the import.
# There may be some tables without a proper primary key and this is a problem
# because then using more than one mapper throws an Exception because the
# table cannot be split properly.
# So we can use this switch to make this transfer done in one mapper instead
# of parallel execution
sqoop import \
 --connect jdbc:mysql://db.hostname:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --num-mappers 1
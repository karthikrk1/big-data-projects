#!/usr/bin/env bash

# This introduces the split-by switch
# This switch can be used in place of the primary key to do the input split
# This switch will use the column provided to split into different sizes
# for doing the Map Reduce.
# This will not be efficient as data can be severely skewed and that can
# lead to difference in execution
# Some points to remember for split-by:
#   * Column should be indexed
#   * Values should be sparse
#   * sequence generated or evenly incremented
#   * This column should not have NULLs

sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --split-by col_other_than_primary_key
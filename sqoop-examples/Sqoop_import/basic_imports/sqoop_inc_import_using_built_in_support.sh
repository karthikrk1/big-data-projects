#!/usr/bin/env bash

# In this sqoop import we use the built-in switches for incremental imports
# The check-column is for the column on which we applied the filter
# The incremental switch is for the mode and here we have append since we
# already have data and would want to append to it.
# The last-value is used to let Sqoop know from which value it should
# compute and append to the folder.
# This mode assumes that we have the last-value stored somewhere
# through which we can run this script as a batch job.

sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --check-column date \
 --incremental append \
 --last-value '2014-01-31' \
 --target-dir /user/karthik/inc_imports \
 -m 2
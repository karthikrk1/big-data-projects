#!/usr/bin/env bash

# The sqoop eval command is used to run queries to the tables
# This is an excellent way to know the structure of the tables
# The connection string is same as the others and uses -P to prompt for
# the password.
# Additionally, here the -e switch takes in a query string that is run
# against the table and produces the results.

sqoop eval \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 -e "SELECT * FROM ORDERS LIMIT 10"

#!/usr/bin/env bash

# This script uses the column switch. This is used along with the
# --table switch. This is used to select only certain columns from the
# table and not all. This is part of a transformation and filtering process.

sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --column id,first_name,last_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 -m 2


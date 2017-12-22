#!/usr/bin/env bash

# This sqoop import introduces the -z switch. Which denotes the compress
# Default compression is gZip which is applied to the sqoop import
# This is highly efficient when table size is huge
sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 -m 2 \
 -z
#!/usr/bin/env bash

# This is one of the most important commands in SQOOP.
# The import command is used to import a table from RDBMS into HDFS
# The connection string is as before.
# Additionally there is a --table switch where we can specify the tables we
# need to import. Also there is a switch --warehouse-dir
# The warehouse-dir can be used to create a common parent folder under which
# the tables can be stored.
# Sqoop creates all the tables in a folder that has the name of the table
# Also warehouse-dir does not give the exception that says
# Target directory exists.
# This can be used to group all tables from the same database in one folder
sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db
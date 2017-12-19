#!/usr/bin/env bash

sqoop import \
 --connect jdbc:mysql://db.hostname:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --num-mappers 1 \
 --delete-target-dir
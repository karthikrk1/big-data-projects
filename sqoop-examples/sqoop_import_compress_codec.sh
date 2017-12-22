#!/usr/bin/env bash

# This script introduces the compression-codec switch.
# This is used to import a Codec to use in the compression step
# If we don't want the default compression; we can use this
# to specify the type of compression we need
sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 -m 2 \
 -z \
 --compression-codec org.apache.hadoop.io.compress.SnappyCodec
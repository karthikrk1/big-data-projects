#!/usr/bin/env bash

# This script introduces the autoreset-to-one-mapper switch
# This switch enables sqoop to auto reset to one mapper if it finds no
# primary key for the table
# This is especially useful when importing multi-tables as this has the
# potential to save the script from exception, in case there is a table
# with no primary key at all.

sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --table table_with_no_primary_key \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --autoreset-to-one-mapper \
 --delete-target-dir

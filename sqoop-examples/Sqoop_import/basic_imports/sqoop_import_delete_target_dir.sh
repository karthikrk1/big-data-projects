#!/usr/bin/env bash

# This is another flavor of the sqoop import with the delete-target-dir
# swtich. This switch is needed when we are importing the same table
# within the warehouse-dir. The import will still fail if the warehouse-dir
# already has another directory with the name of the table we are trying
# to import. If so this switch can be enabled to delete the target dir
# before importing to the location
sqoop import \
 --connect jdbc:mysql://db.hostname:3306/db \
 --username user \
 -P \
 --table table_name \
 --warehouse-dir /user/karthik/sqoop_import/db \
 --num-mappers 1 \
 --delete-target-dir
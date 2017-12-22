#!/usr/bin/env bash

# This introduces the --query switch to be used with the import.
# This is another form of transformation and filtering and
# This is mutually exclusive to the table and query switch
# Some notes:
# * query cannot be used with table and column
# * warehouse-dir cannot be used since the table is not imported directly
# * if num-mappers > 1 then split-by is mandatory since this may involve more
#   than one table.
# * The \$CONDITIONS is mandatory in the where clause and $ is a special 
#   character and is escaped with a '\'
sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --target-dir /user/karthik/test_query \
 -m2 \
 --query "select a.name, b.address from person a, address b
 where a.id = b.person_id and \$CONDITIONS" \
 --split-by id

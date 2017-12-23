#!/usr/bin/env bash

# This is part 1 of incremental imports.
# Here we use the query switch to import only data from table where the date
# was in the year 2013. Since we use query; we use split-by and \$CONDITIONS
# and the target-dir
# In the next part we'll see mechanisms to incrementally import from the
# point where we left off (i.e.) the data till the year 2013.
sqoop import \
 --connect jdbc:mysql://db.hostname.com:3306/db \
 --username user \
 -P \
 --query "select * from table_name where date like '2013-%' and \$CONDITIONS" \
 --target-dir /user/karthik/inc_imports \
 -m 2 \
 --split-by id

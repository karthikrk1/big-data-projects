#!/bin/bash

# This command lists all tables inside the specified database
# The database name is specified in the connection URL after the port
# -P argument is to enter the password on the terminal when the file
# is run. This is to be safe so as to not have clear text passwords
sqoop list-tables \
 --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
 --username retail_user \
 -P
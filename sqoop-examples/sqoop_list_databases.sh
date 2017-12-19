#!/bin/bash

# This script is used to connect to the Database instance of the cluster
# and to list the databases that are currently configured
# I am using itversity's cluster to run and this is their hostname
# Just replace the hostname with other hostname, the username
# and password

sqoop list-databases \
 --connect jdbc:mysql://ms.itversity.com:3306 \
 --username retail_user \
 -P
#!/bin/bash

# To start a Spark-shell with custom ui port

spark-shell --master yarn --conf spark.ui.port=<any 5 digit portnumber>

# example:

spark-shell --master yarn --conf spark.ui.port=10948

# Initialize with custom num-executors and executor-memory
# This is needed if we have less data to process and allocate more
# or we have more data and allocate less memory

spark-shell --master yarn \
    --conf spark.ui.port=10948 \
    --num-executors 1 \
    --executor-memory 512M

# this talks about opening a python shell for spark

pyspark 

# This will open a Python shell where we can execute spark commands via Python

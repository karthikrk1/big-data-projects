#!/bin/bash

# To start a Spark-shell with custom ui port

spark-shell --master yarn --conf spark.ui.port=<any 5 digit portnumber>

# example:

spark-shell --master yarn --conf spark.ui.port=10948

# 
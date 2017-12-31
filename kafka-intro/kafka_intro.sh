# Create a topic
kafka-topics.sh \
  --create \
  --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
  --replication-factor 1 \
  --partitions 1 \
  --topic kafkademokrk

# List to validate a topic

kafka-topics.sh \
  --list \
  --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
  --topic kafkademokrk

# Produce to a topic

kafka-console-producer.sh \
  --broker-list nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
  --topic kafkademokrk
  
# Consume from a topic

kafka-console-consumer.sh \
  --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
  --bootstrap-server nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
  --topic kafkademokrk \
  --from-beginning

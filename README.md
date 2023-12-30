# StockStats

Demo StockStas using Kafka stream.

## Configuration

```
groupId=com.github.quangtn.kafka.streams
artifactId=kafka_stocks_project
version=1.0-SNAPSHOT
```

## Running

1. Generate topics from Kafka CLI.

```
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic stocks --partitions 1 --replication-factor 1

kafka-topics.sh --bootstrap-server localhost:9092 --create --topic stockstats-output --partitions 1 --replication-factor 1

kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic stockstats-output \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \

```

2. Running StockGenProducer.java (Produce stream) && StockStatsExample.java (Start Application)

# Flink Kafka Consumer

This Java program consumes string messages from Apache Kafka with Apache Flink's
`KafkaSource` and prints each received message to the console. It connects to
`localhost:9092`, subscribes to `events_realtime1`, and uses the consumer group
`flink-rest-consumer`.

The Flink job enables checkpointing every 10 seconds, uses exactly-once
checkpointing mode, sets a 60-second checkpoint timeout, and allows one
concurrent checkpoint. The job is started with `Kafka Consumer Job`.

## Technologies

- Java 11
- Apache Maven
- Apache Flink 1.19.1
- Flink Kafka connector 3.2.0-1.19
- Kafka clients 3.7.0
- Jackson Databind 2.17.2

## Kafka commands

These are the Kafka commands used with the project:

1. Start Kafka:

   ```bash
   cd bin/; cd kafka/; ./kafka-server-start.sh /home/lokesh_kumar/kafka/config/kraft/server.properties
   ```

2. Create the Kafka topic:

   ```bash
   cd ~/kafka; bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events_realtime --partitions 3 --replication-factor 1
   ```

3. Start the Kafka producer:

   ```bash
   bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events_realtime1
   ```

4. Start the Kafka consumer:

   ```bash
   cd ~/kafka; bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events_realtime --from-beginning
   ```

## Topic name note

The topic names in the supplied commands are not consistent: the topic-creation
and console-consumer commands use `events_realtime`, while the console-producer
command and the Flink program use `events_realtime1`. The existing commands and
program are documented as written; none have been changed here.

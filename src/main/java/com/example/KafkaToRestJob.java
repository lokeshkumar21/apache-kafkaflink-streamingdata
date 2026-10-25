package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaToRestJob {

    public static void main(String[] args) throws Exception {

        String kafkaServers = "localhost:9092";

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint configuration
        env.enableCheckpointing(10000);

        env.getCheckpointConfig().setCheckpointingMode(
                CheckpointingMode.EXACTLY_ONCE
        );

        env.getCheckpointConfig().setCheckpointTimeout(60000);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Configure checkpoint storage if provided
//        String checkpointDir = System.getenv("CHECKPOINT_DIR");
//
//        if (checkpointDir != null && !checkpointDir.trim().isEmpty()) {
//            env.getCheckpointConfig()
//                    .setCheckpointStorage(checkpointDir);
//        }

        // Kafka source configuration
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics("events_realtime1")
                .setGroupId("flink-rest-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Read messages from Kafka
        DataStream<String> kafkaMessages = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Print messages to the console
        kafkaMessages.print();

        // Execute the Flink job
        env.execute("Kafka Consumer Job");
    }
}
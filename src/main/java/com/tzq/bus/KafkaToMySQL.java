package com.tzq.bus;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Title
 * @Author zhengqiang.tan
 * @Date 4/9/23 5:17 PM
 */
public class KafkaToMySQL {

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);

        // set up the Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test-group");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // transform and write the data to MySQL using JDBC sink
        kafkaSource
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        // transform the data into a Tuple2<String, Integer> object
                        String[] fields = value.split(",");
                        return new Tuple2<>(fields[0], Integer.valueOf(fields[1]));
                    }
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO test_table (name, value) VALUES (?, ?)",
                        (ps, tuple) -> {
                            ps.setString(1, tuple.f0);
                            ps.setInt(2, tuple.f1);
                        },
                        new JdbcExecutionOptions.Builder().withBatchSize(100)
                                .withMaxRetries(3).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUrl("jdbc:mysql://localhost:3306/test")
                                .withUsername("user")
                                .withPassword("password")
                                .build()));

        // execute the program
        env.execute("KafkaToMySQL");
    }
}

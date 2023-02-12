package com.flinkcore.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:xmzhou
 * @Date: 2022/7/30 14:50
 * @Description: flink 整合 kafka
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hdp01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        env.socketTextStream("localhost", 9999).sinkTo(kafkaSink);
        env.execute();
    }
}

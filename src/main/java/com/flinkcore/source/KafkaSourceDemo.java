package com.flinkcore.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:xmzhou
 * @Date: 2022/7/30 14:22
 * @Description: Flink整合kafka
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hdp01:9092")
                .setTopics("flinksql")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // flink会把kafka消费者的位移记录在算子的状态中，这样就实现了消费位移状态的容错，从而可以支持端到端的一致性


        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka-source").print();
        env.execute();

    }
}

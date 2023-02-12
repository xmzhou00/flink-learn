package com.flinkcore.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/12 15:28
 * @Description: mysql cdc
 */
public class MysqlSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("flink") // set captured database
                .tableList("flink.t_test") // set captured table
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();


        // 开启checkpoint
        env.enableCheckpointing(3000);


        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc source")
                .print();

        env.execute();


    }
}

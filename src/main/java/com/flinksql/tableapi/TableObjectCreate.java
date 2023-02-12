package com.flinksql.tableapi;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author:xmzhou
 * @Date: 2022/8/1 12:32
 * @Description: flink sql table api的使用\
 * <p>
 * Table 对象创建的方式
 * </p>
 */
public class TableObjectCreate {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table table = null;
        /*
            假设已经存在一张表
           tEnv.executeSql("create table t_a ...")
         */

        /**
         * 一、从已存在的表，来创建Table对象
         */
        // Table table = tEnv.from("t_a");

        /**
         * 二、从一个tableDescriptor来创建Table对象
         */
    /*    table = tEnv.from(TableDescriptor
                .forConnector("kafka") // 指定连接器
                .schema(Schema.newBuilder() // 指定表结构
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("json")  // 指定数据源的数据格式
                .option("topic", "flinksql")  // 连接器及format格式的相关参数
                .option("properties.bootstrap.servers", "hdp01:9092")
                .option("properties.group.id", "g2")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());*/


        /**
         * 三、从数据流来创建Table对象
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("flinksql")
                .setBootstrapServers("hdp01:9092")
                .setGroupId("aa")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka");

        // 1. 不指定Schema，将流创建成table对象，表的schema时默认的
        // table = tEnv.fromDataStream(kafkaStream);

        // 2. 为了得到理想的表结构，可以将流转换成javabean，这样就可以通过反射获取schema信息
        SingleOutputStreamOperator<Person> personStream = kafkaStream.map(json -> JSON.parseObject(json, Person.class));
       /*
        personStream.print();
       table =  tEnv.fromDataStream(personStream);
       table.select($("f0")).execute().print();

        table.printSchema();*/

        // 3. 手动指定schema，将一个javabean对象，转换成Table对象
        table = tEnv.fromDataStream(personStream, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("gender", DataTypes.STRING())
                .build());

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person {
        public int id;
        public String name;
        public int age;
        public String gender;
    }
}

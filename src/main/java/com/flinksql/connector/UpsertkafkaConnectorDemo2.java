package com.flinksql.connector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/3 12:39
 * @Description: 通过两个流的join 来观察 upsert kafka的 +I -U +U -D等操作
 */
public class UpsertkafkaConnectorDemo2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        tEnv.getConfig().getConfiguration().setString("table.exec.sink.not-null-enforcer","drop");

        // 1,male
        SingleOutputStreamOperator<Bean1> bean1 = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Bean1>() {
                    @Override
                    public Bean1 map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Bean1(Integer.parseInt(fields[0]), fields[1]);
                    }
                });


        // 1,zs
        SingleOutputStreamOperator<Bean2> bean2 = env.socketTextStream("localhost", 9998)
                .map(new MapFunction<String, Bean2>() {
                    @Override
                    public Bean2 map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Bean2(Integer.parseInt(fields[0]), fields[1]);
                    }
                });

        // 流转表
        tEnv.createTemporaryView("bean1", bean1);
        tEnv.createTemporaryView("bean2", bean2);

//        tEnv.executeSql("select * from bean1 left join bean2 on bean1.id = bean2.id").print();

        // 创建结果表
        // 需要定义主键
        tEnv.executeSql("" +
                "create table t_join(\n" +
                "id int primary key not enforced,\n" +
                "name string,\n" +
                "gender string\n" +
                ")with(\n" +
                "\t'connector' = 'upsert-kafka',\n" +
                "\t'topic' = 'topic-upsert2',\n" +
                "\t'properties.bootstrap.servers' = 'hdp01:9092',\n" +
                "\t'key.format' = 'json',\n" +
                "\t'value.format' = 'json'\n" +
                ")");

        // 将数据写入到kafka
        tEnv.executeSql("insert into t_join select a.id,name,gender from bean1 a left join bean2 b on a.id = b.id");

        // 查看表中的数据
        tEnv.executeSql("select * from t_join").print();
        /*
        +----+-------------+--------------------------------+--------------------------------+
        | op |          id |                           name |                         gender |
        +----+-------------+--------------------------------+--------------------------------+
        | +I |           1 |                         (NULL) |                           male |
        | -D |           1 |                         (NULL) |                           male |
        | +I |           1 |                             zs |                           male |
           */

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1 {
        public int id;
        public String gender;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean2 {
        public int id;
        public String name;
    }
}

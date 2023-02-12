package com.flinksql.connector;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/2 21:44
 * @Description:  upsert kafka connector
 */
public class UpsertKafkaConnectorDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv  = StreamTableEnvironment.create(env);

        // 流1：1,male
        SingleOutputStreamOperator<Bean1> bean1  = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Bean1>() {
                    @Override
                    public Bean1 map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Bean1(Integer.parseInt(fields[0]), fields[1]);
                    }
                });
        // 转换成一张表
        tEnv.createTemporaryView("bean1",bean1);
        // 通过group by后可以得到 +I -U +U的流
//        tEnv.executeSql("select gender,count(1) as ctn from bean1 group by gender").print();

        // 创建upsert kafka表
        tEnv.executeSql("" +
                "create table t_gender(\n" +
                "\tgender string primary key not enforced,\n" +
                "\tctn bigint\n" +
                ")with(\n" +
                "\t'connector' = 'upsert-kafka',\n" +
                "\t'topic' = 'topic-upsert',\n" +
                "\t'properties.bootstrap.servers' = 'hdp01:9092',\n" +
                "\t'key.format' = 'json',\n" +
                "\t'value.format' = 'json'\n" +
                ")");

// 写入数据
        tEnv.executeSql("insert into t_gender select gender,count(1) from bean1 group by gender");

        tEnv.executeSql("select * from t_gender").print();

    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1{
        public int id;
        public String gender;
    }

}

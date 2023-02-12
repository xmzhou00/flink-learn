package com.flinksql.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/2 20:00
 *
 */
public class KafkaConnectorDemo {
    public static void main(String[] args) {
        TableEnvironment tEnv  = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

       /* tEnv.executeSql("" +
                "create table t_event(\n" +
                "\tguid int,\n" +
                "\teventId string,\n" +
                "\teventTime bigint,\n" +
                "\tk1 string,\n" +
                "\taa int,\n" +
                "\tbb string\n" +
                ")WITH(\n" +
                "\t'connector' = 'kafka',\n" +
                "\t'topic' = 'aaa',\n" +
                "\t'key.format' = 'json',\n" +
                "\t'key.fields' = 'aa;bb',\n" +
                "\t'value.format' = 'json',\n" +
                "\t'properties.bootstrap.servers' = 'hdp01:9092',\n" +
                "\t'properties.group.id' = 'g1',\n" +
                "\t'key.json.ignore-parse-errors' = 'true',\n" +  // json解析错误跳过
                "\t'value.fields-include' = 'except_key',\n" + // 表中定义的kafka record key中的字段是否会在 kafka key 和 value中同时存在 同时存在就是 ALL
                "\t'scan.startup.mode' = 'earliest-offset'\n" +
                ")");*/

//        tEnv.executeSql("select * from t_event").print();


            tEnv.executeSql("" +
                    "create table t_event2(\n" +
                    "\tguid int,\n" +
                    "\teventId string,\n" +
                    "\teventTime bigint,\n" +
                    "\tk1 int,\n" +
                    "\tk2 string,\n" +
                    "\tkey_k1 int,\n" +
                    "\tkey_k2 string\n" +
                    ")WITH(\n" +
                    "\t'connector' = 'kafka',\n" +
                    "\t'topic' = 'bbb',\n" +
                    "\t'key.format' = 'json',\n" +
                    "\t'key.fields-prefix' = 'key_',\n" +
                    "\t'key.fields' = 'key_k1;key_k2',\n" +
                    "\t'key.json.ignore-parse-errors' = 'true',\n" +
                    "\t'value.format' = 'json',\n" +
                    "\t'value.fields-include' = 'except_key',\n" +
                    "\t'value.json.ignore-parse-errors' = 'true',\n" +
                    "\t'properties.bootstrap.servers' = 'hdp01:9092',\n" +
                    "\t'properties.group.id' = 'g2',\n" +
                    "\t'scan.startup.mode' = 'earliest-offset'\n" +
                    ")");

            tEnv.executeSql("select * from t_event2").print();





    }
}

package com.flinksql.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/12 15:25
 * @Description: flink-sql mysql cdc
 */
public class MysqlSourceDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv  = StreamTableEnvironment.create(env);
        env.enableCheckpointing(3000); // 需要开启checkpoint
        tEnv.executeSql("" +
                "CREATE TABLE orders (\n" +
                "     id int primary key not enforced,\n" +
                " line string\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'localhost',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '123456',\n" +
                "     'database-name' = 'flink',\n" +
                "     'table-name' = 't_test'\n" +
                "\t )");

        tEnv.executeSql("select * from orders").print();

    }
}

package com.flinksql.sqlapi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author:xmzhou
 * @Date: 2022/8/2 10:07
 * @Description:
 */
public class ColumnDetail {
    public static void main(String[] args) {

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 建表（数据源表）
        // {"id":4,"name":"zs","nick":"aa","age":18,"gender":"male"}
        tEnv.executeSql("create table t_person(\n" +
                "\tid int,\n" +
                "\tname string,\n" +
                "\tnick string,\n" +
                "\tage int,\n" +
                "\tage_exp as age + 10,\n" +
                "\tgender string,\n" +
                "\toffs bigint metadata from 'offset',\n" +
                "\t`timestamp` timestamp_ltz(3) metadata\n" +
                ")WITH(\n" +
                "\t'connector' = 'kafka',\n" +
                "\t'topic' = 'columnDetail',\n" +
                "\t'format' = 'json',\n" +
                "\t'properties.bootstrap.servers' = 'hdp01:9092',\n" +
                "\t'scan.startup.mode' = 'earliest-offset',\n" +
                "\t'properties.group.id' = 'aa'\n" +
                ")");

        tEnv.executeSql("select * from t_person").print();


    }
}

package com.flinksql.quickstart;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author:xmzhou
 * @Date: 2022/8/1 8:37
 * @Description: flink sql 开发步骤
 * <p>
 *     1. 创建flinksql编程入口
 *     2. 将数据源映射成表
 *     3. 执行sql查询语句（sql语法或者table api）
 *     4. 将查询结果输出到目标表
 * </p>
 */
public class FlinkSqlDemo {

    public static void main(String[] args) {

        TableEnvironment tableEnv  = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // 把kafka中的topic映射成一张表
        // {"id":1,"name":"zs","age":28,"gender":"male"}

        tableEnv.executeSql(
                "create table t_kafka                                  "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'flinksql',                              "
                        + "  'properties.bootstrap.servers' = 'hdp01:9092',   "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );

        tableEnv.executeSql("select * from t_kafka").print();


    }
}

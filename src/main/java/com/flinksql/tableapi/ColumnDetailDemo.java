package com.flinksql.tableapi;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author:xmzhou
 * @Date: 2022/8/1 22:10
 * @Description: schema定义详解
 */
public class ColumnDetailDemo {
    public static void main(String[] args) {
        TableEnvironment tEnv   = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 建表（数据源表）
        // {"id":4,"name":"zs","nick":"aa","age":18,"gender":"male"}
        Table table = tEnv.from(TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())  // column是声明物理字段到表结构中来
                        .column("name", DataTypes.STRING())
                        .column("nick", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .columnByExpression("age_exp","age+10") // 表达式字段
                        // isVirtual 是表示： 当这个表被作为sink表时，该字段是否出现在schema中
                        .columnByMetadata("offs",DataTypes.BIGINT(),"offset",true)
                        .build())
                .format("json")
                .option("topic", "columnDetail")
                .option("properties.bootstrap.servers", "hdp01:9092")
                .option("properties.group.id", "g1")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")

                .build());
        table.printSchema();

        table.select($("id"),$("name"),$("nick"),$("age"),$("age_exp"),$("gender"),$("offs")).execute().print();


    }
}

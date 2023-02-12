package com.flinksql.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.DateType;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author:xmzhou
 * @Date: 2022/8/1 9:48
 * @Description: flink table api
 */
public class TableApiDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv  = StreamTableEnvironment.create(env);

        // 指定数据源
        Table table = tableEnv.from(TableDescriptor
                .forConnector("kafka") // 指定连接器
                .schema(Schema.newBuilder() // 指定表结构
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("json") // 指定数据源格式
                .option("topic", "flinksql")  // 连接器及format格式的相关参数
                .option("properties.bootstrap.servers", "hdp01:9092")
                .option("properties.group.id", "g2")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());


        // table api 方式查询
        table.select($("name"),$("id"),$("gender")).execute().print();

    }
}

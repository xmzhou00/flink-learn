package com.flinksql.sqlapi;

import com.flinksql.tableapi.TableObjectCreate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.temporal.Temporal;

/**
 * @Author:xmzhou
 * @Date: 2022/8/1 13:09
 * @Description: flink sql sql api方式创建表
 */
public class TableSqlCreate {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        /**
         * 一、通过构建一个TableDescriptor来创建一个表
         */
        tEnv.createTable("table_a", TableDescriptor
                .forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("csv")
                .option("path","src\\data")
                .build());
    tEnv.executeSql("select * from table_a").print();

      /*  *//**
         * 二、从一个datastream上创建 视图
         *//*

        SingleOutputStreamOperator<TableObjectCreate.Person> personStream  = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, TableObjectCreate.Person>() {
                    @Override
                    public TableObjectCreate.Person map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new TableObjectCreate.Person(Integer.parseInt(fields[0]), fields[1], Integer.parseInt(fields[2]), fields[3]);
                    }
                });
        tEnv.createTemporaryView("table_b",personStream);
        tEnv.executeSql("select * from table_b").print();


        *//**
         * 三、从一个已存在的table对象 创建一个视图
         *//*
        Table table_a = tEnv.from("table_a");
        tEnv.createTemporaryView("t_a",table_a);*/


    }
}

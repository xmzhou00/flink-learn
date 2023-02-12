package com.flinksql.format;

import org.apache.flink.table.api.*;

/**
 * @Author:xmzhou
 * @Date: 2022/8/2 10:21
 * @Description: 主要时熟悉嵌套json的处理方式
 */
public class JsonFormatDemo {
    public static void main(String[] args) {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        /**
         * 简单嵌套json，
         * 嵌套json解析成map类型
         * {"id":12,"name":{"nick":"doe3","formal":"doit edu3","height":170}}
         */

        tEnv.executeSql("" +
                "create table t_person(\n" +
                "\tid int,\n" +
                "\tname map<string,string>\n" +
                ")WITH(\n" +
                "\t'connector' = 'filesystem',\n" +
                "\t'format' = 'json' ,\n" +
                "\t'path' = 'file:///D:\\javaSource\\flinkSource\\Apache_Flink_Code\\src\\json'\n" +
                ")");

        // tEnv.executeSql("desc t_person").print();
        // tEnv.executeSql("select * from t_person").print();
        // tEnv.executeSql("select name['nick'] from t_person").print();


        /**
         * 二、简单嵌套json
         * 嵌套json，解析成row类型
         * row 类似于hive中的 struct
         *  {"id":12,"name":{"nick":"doe3","formal":"doit edu3","height":170}}
         */
        tEnv.createTable("t_person2", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.ROW(
                                DataTypes.FIELD("nick", DataTypes.STRING()),
                                DataTypes.FIELD("formal", DataTypes.STRING()),
                                DataTypes.FIELD("height", DataTypes.INT())
                        ))
                        .build())
                .format("json")
                .option("path","src/json/")
                .build());

        // tEnv.executeSql("desc t_person2").print();
        // tEnv.executeSql("select * from t_person2").print();
        // tEnv.executeSql("select name.nick from t_person2").print();


        /**
         * 三、复杂json
         * {"id":1,"friends":[{"name":"a","info":{"addr":"bj","gender":"male"}},{"name":"b","info":{"addr":"sh","gender":"female"}}]}
         *
         */

        tEnv.executeSql("" +
                "create table t_json(\n" +
                "\tid int,\n" +
                "\tfriends array<row<name string,info map<String,string> >>\n" +
                ")WITH(\n" +
                "\t'connector' = 'filesystem',\n" +
                "\t'format' = 'json',\n" +
                "\t'path' = 'src/json2/'\n" +
                ")");


        tEnv.executeSql("desc t_json").print();
        tEnv.executeSql("select * from t_json").print();
        // array下标从1开始
        tEnv.executeSql("select friends[1].name, friends[1].info from t_json").print();


    }
}

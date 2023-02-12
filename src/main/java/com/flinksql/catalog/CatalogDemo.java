package com.flinksql.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author:xmzhou
 * @Date: 2022/8/1 13:54
 * @Description: catalog测试
 */
public class CatalogDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        // 环境创建之初，底层就自动初始化了一个默认的元数据空间实现对象(default_catalog ==> GenericInMemoryCatalog)
        StreamTableEnvironment tEnv  = StreamTableEnvironment.create(env);



        String catalogTypeName = "hive";
        String defaultDataBase = "default";
        String hiveConfDir = "src\\hiveconf";

        // 创建和注册hive catalog
        HiveCatalog hiveCatalog = new HiveCatalog(catalogTypeName, defaultDataBase, hiveConfDir);
        // 将hive元数据空间对象注册到环境中
        // 目前环境中就拥有了两套catalog
        tEnv.registerCatalog("myHive",hiveCatalog);

        // tEnv.useCatalog("myHive");
        // tEnv.executeSql("show catalogs").print();
        // tEnv.executeSql("show databases").print();


        /**
         * 注意：如果此处是临时表的话，是不会存储到hive的catalog中的，只有常规表才能保存到hive catalog中
         */
       /* tEnv.executeSql(
                "create  table `myHive`.`default`.`t_kafka`    "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'flinksql',                              "
                        + "  'properties.bootstrap.servers' = 'hdp01:9092',     "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );*/

     /*   tEnv.executeSql(
                "create temporary table `t_kafka2`    "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'flinksql',                              "
                        + "  'properties.bootstrap.servers' = 'hdp01:9092',     "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );*/

        // tEnv.executeSql("select * from ")


        // 列出当前会话中的所有catalog
        tEnv.listCatalogs();
        // 使用默认的catalog
        tEnv.executeSql("use catalog default_catalog");
        tEnv.executeSql("show databases").print();

        System.out.println("--------------------");

        tEnv.executeSql("use catalog myHive");
        tEnv.executeSql("show databases").print();
        // tEnv.executeSql("use default");
        tEnv.executeSql("show tables").print();

    }
}

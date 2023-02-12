package com.flinksql.query;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/5 14:32
 * @Description:
 */
public class LookupJoinDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /**
         * 1,a
         * 2,b
         * 3,c
         * 4,d
         * 5,e
         */
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<Integer, String>> ss1 = s1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(Integer.parseInt(arr[0]), arr[1]);
        }).returns(new TypeHint<Tuple2<Integer, String>>() {
        });


        // 创建主表
        tenv.createTemporaryView("a",ss1, Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1",DataTypes.STRING())
                        .columnByExpression("pt","proctime()")
                .build());

        // 创建维表（Lookup）
        tenv.executeSql(
                "create table b(   \n" +
                        "   id  int  , \n" +
                        "   name string, \n" +
                        "   gender STRING, \n" +
                        "   primary key(id) not enforced  \n" +
                        ") with (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://localhost:3306/flink',\n" +
                        "  'table-name' = 't_user',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '123456' \n" +
                        ")"
        );


        // Lookup join 查询
            tenv.executeSql("select a.*,c.* from a join b for system_time as of a.pt as c on a.f0 = c.id").print();





    }
}

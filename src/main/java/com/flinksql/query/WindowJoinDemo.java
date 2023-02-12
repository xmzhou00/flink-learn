package com.flinksql.query;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/4 20:53
 * @Description: 各种窗口join案例
 */
public class WindowJoinDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /**
         * 流1
         * 1,a,1000
         * 2,b,2000
         * 3,c,2500
         * 4,d,3000
         * 5,e,5000
         */
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.socketTextStream("localhost", 9998)
                .map(new MapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple3.of(fields[0], fields[1], Long.parseLong(fields[2]));
                    }
                });


        /**
         * 流2
         * 1,bj,1000
         * 2,sh,2000
         * 4,xa,2600
         * 5,yn,5000
         */
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple3.of(fields[0], fields[1], Long.parseLong(fields[2]));
                    }
                });

        // 创建表

        tenv.createTemporaryView("t1", stream1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2,3)")
                .watermark("rt", "rt - interval '0' second")
                .build());

        tenv.createTemporaryView("t2", stream2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2,3)")
                .watermark("rt", "rt - interval '0' second")
                .build());


        // inner join
        // left | right | full outer join

        /*tenv.executeSql("" +
                "select \n" +
                "\tL.f0,L.f1,L.f2,R.f1\n" +
                "from \n" +
                "( select * from table(tumble(table t1,descriptor(rt),interval '5' seconds))) L \n" +
                "\tjoin\n" +
                "( select * from table(tumble(table t2,descriptor(rt),interval '5' seconds))) R\n" +
                "on L.window_start = R.window_start and L.window_end = R.window_end and L.f0 = R.f0")*//*.print()*//*;*/

        // semi join ==> where ... in ...
        // semi join只能查看左表的字段

        tenv.executeSql(
                "SELECT\n" +
                        "\ta.f0,a.f1,a.f2\n" +
                        "FROM\n" +
                        "( select * from table(tumble(table t1,descriptor(rt),interval '5' seconds)) ) a\n" +
                        "where f0 in \n" +
                        "(\n" +
                        "\tselect f0 FROM\n" +
                        "\t\t(select * from table(tumble(table t2,descriptor(rt),interval '5' seconds)) ) b\n" +
                        "\t\twhere a.window_start = b.window_start and a.window_end = b.window_end\n" +
                        ")"
        ).print();

        // anti join ==> where ... not in ...
        // anti join只能查看左表的字段

        tenv.executeSql(
                "SELECT\n" +
                        "\ta.f0,a.f1,a.f2\n" +
                        "FROM\n" +
                        "( select * from table(tumble(table t1,descriptor(rt),interval '5' seconds)) ) a\n" +
                        "where f0 not in \n" +
                        "(\n" +
                        "\tselect f0 FROM\n" +
                        "\t\t(select * from table(tumble(table t2,descriptor(rt),interval '5' seconds)) ) b\n" +
                        "\t\twhere a.window_start = b.window_start and a.window_end = b.window_end\n" +
                        ")"
        ).print();

    }
}

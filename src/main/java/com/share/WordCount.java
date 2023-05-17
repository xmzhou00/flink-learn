package com.share;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        func1(env);

        func2(env);
        env.execute();
    }

    private static void func1(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.fromElements("aa,bb", "aa,cc", "aa,dd");
        source.flatMap((String str, Collector<String> out) -> {
                    for (String word : str.split(",")) {
                        out.collect(word);
                    }
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1)
                .print("result");
    }

    private static void func2(StreamExecutionEnvironment env) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String ddl = "CREATE TABLE datagen (\n" +
                " str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.str.length'='5'\n" +
                ")";
        tableEnv.executeSql(ddl);

//        tableEnv.executeSql("select * from datagen").print();

        tableEnv.executeSql("select str, count(*) as cnt from datagen group by str").print();

    }
}

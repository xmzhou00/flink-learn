package com.flinkcore.timer;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

/**
 * Flink Timer demo
 */
public class TimerDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> source = env.addSource(
                new DataGeneratorSource<Tuple2<String, Integer>>(
                        new RandomGenerator<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> next() {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                return new Tuple2<>("" + random.nextInt(1, 5), random.nextInt(1, 100000));
                            }
                        }, 10, 10000L
                )
        ).returns(Types.TUPLE(Types.STRING, Types.INT));

        source.keyBy(t -> t.f0)
                .process(new KeyedIntProcess())
                .print();

        env.execute();


    }

}

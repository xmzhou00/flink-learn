package com.flinkcore.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:xmzhou
 * @Date: 2022/6/5 20:33
 * @Description: KeyedState中一些state api 演示
 */
public class StateAPI {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> input = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[0], Integer.valueOf(fields[1]));
            }
        });


        // testReducingState(input);

        testAggregatingState(input);


        env.execute();
    }

    /**
     * ReducingState测试
     *
     * @param input
     */
    public static void testReducingState(SingleOutputStreamOperator<Tuple2<String, Integer>> input) {

        input.keyBy(t -> t.f0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Integer>() {

                    // 定义状态
                    private ReducingState<Integer> reducingState;

                    @Override
                    public Integer map(Tuple2<String, Integer> tuple2) throws Exception {
                        reducingState.add(tuple2.f1);
                        return reducingState.get();
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 描述器中需要定义聚合规则，也就是自定义函数
                        // 注意: ReduceFunction只有一个泛型
                        ReducingStateDescriptor<Integer> reducingStateDescriptor = new ReducingStateDescriptor<Integer>("count", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer t1, Integer t2) throws Exception {
                                return t1 + t2;
                            }
                        }, Integer.class);
                        reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
                    }
                })
                .print();

    }

    public static void testAggregatingState(SingleOutputStreamOperator<Tuple2<String, Integer>> input) {
        input.keyBy(t -> t.f0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Integer>() {

                    private AggregatingState<Integer, Integer> aggregatingState;

                    @Override
                    public Integer map(Tuple2<String, Integer> tuple2) throws Exception {

                        aggregatingState.add(tuple2.f1);

                        return aggregatingState.get();
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Integer> descriptor = new AggregatingStateDescriptor<>("avg", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Integer>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0, 0);
                            }

                            @Override
                            public Tuple2<Integer, Integer> add(Integer input, Tuple2<Integer, Integer> acc) {
                                return Tuple2.of(acc.f0 + input, acc.f1 + 1);
                            }

                            @Override
                            public Integer getResult(Tuple2<Integer, Integer> state) {
                                return state.f0 / state.f1;
                            }

                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc1, Tuple2<Integer, Integer> acc2) {
                                return Tuple2.of(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
                            }
                        }, TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                        }));
                        aggregatingState = getRuntimeContext().getAggregatingState(descriptor);
                    }
                }).print();

    }

}

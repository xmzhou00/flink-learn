package com.flinkcore.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author:xmzhou
 * @Date: 2022/7/31 11:49
 * @Description: flink实现广播
 * 案例：
 * 流1：用户行为事件流（持续不断，同一个人也会反复出现，出现次数不定
 * 流2：用户维度信息（年龄，城市），同一个人的数据只会来一次，来的时间也不固定（作为广播流）
 * 将用户事件流关联用户维度信息。
 */
public class BroadCastDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 用户行为事件流
        // id,event
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple2.of(fields[0], fields[1]);
                    }
                });


        // 用户维度信息
        // id,age,city
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> s2 = env.socketTextStream("localhost", 9998)
                .map(new MapFunction<String, Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple3.of(fields[0], Integer.parseInt(fields[1]), fields[2]);
                    }
                });


        // 将用户维度信息转换成广播流
        // 使用状态存储维度信息
        MapStateDescriptor<String, Tuple2<Integer, String>> userInfoState = new MapStateDescriptor<>("userInfoState", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
        }));
        BroadcastStream<Tuple3<String, Integer, String>> broadcastStream = s2.broadcast(userInfoState);


        // 哪个流需要使用广播状态数据，那么就去 connect 这个广播流
        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, Integer, String>> connectedStream = s1.connect(broadcastStream);
        /**
         * 对连接了广播流之后的流进行处理
         * 核心思想：
         * - 在processBroadcastElement方法中，把获取到的广播流中的数据，插入到 “广播状态”中
         * - 在processElement方法中，对取到的主流数据进行处理（从广播状态中获取要拼接的数据，拼接后输出）
         */

        connectedStream.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, Integer, String>, String>() {
            /**
             * 处理主流中的每条数据
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 此处获取的时一个只读对象
                ReadOnlyBroadcastState<String, Tuple2<Integer, String>> broadcastState = ctx.getBroadcastState(userInfoState);
                if (broadcastState!=null){
                    Tuple2<Integer, String> userInfo = broadcastState.get(value.f0);
                    out.collect(value.f0+"--"+userInfo);
                }else{
                    out.collect(value.f0+"-- null");
                }
            }

            /**
             * 处理广播流
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Tuple3<String, Integer, String> value, Context ctx, Collector<String> out) throws Exception {
                // 从上下文中 获取广播状态（可读可写的）
                BroadcastState<String, Tuple2<Integer, String>> broadcastState = ctx.getBroadcastState(userInfoState);
                broadcastState.put(value.f0, Tuple2.of(value.f1, value.f2));
            }
        })
                .print();


        env.execute();
    }
}

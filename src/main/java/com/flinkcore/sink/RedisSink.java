//package com.flinkcore.sink;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.redis.RedisSink;
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
//
///**
// * @Author:xmzhou
// * @Date: 2022/7/30 21:15
// * @Description: 由于maven中央仓库中flink-connector-redis 版本较老，所以自己手动编译了一个适配的jar包
// */
//class RedisSinkDemo {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//
//        // 定义redis连接配置
//        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
//                .setHost("127.0.0.1")
//                .build();
//
//
//        env.socketTextStream("localhost", 9999)
//                .map(new MapFunction<String, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> map(String value) throws Exception {
//                        String[] fields = value.split(",");
//                        return Tuple2.of(fields[0], fields[1]);
//                    }
//                })
//                .addSink(new RedisSink<Tuple2<String, String>>(flinkJedisPoolConfig, new RedisExampleMapper()));
//
//
//        env.execute();
//    }
//
//    static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {
//
//        @Override
//        public RedisCommandDescription getCommandDescription() {
//            return new RedisCommandDescription(RedisCommand.HSET, "hash-test");
//        }
//
//        @Override
//        public String getKeyFromData(Tuple2<String, String> data) {
//            return data.f0;
//        }
//
//        @Override
//        public String getValueFromData(Tuple2<String, String> data) {
//            return data.f1;
//        }
//    }
//}

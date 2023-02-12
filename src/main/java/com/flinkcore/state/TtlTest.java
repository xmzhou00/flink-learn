package com.flinkcore.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:xmzhou
 * @Date: 2022/6/5 21:47
 * @Description:
 */
public class TtlTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
                .keyBy(x -> "0") // 数据全部发往一个subtask
                .map(new RichMapFunction<String, String>() {

                    ListState<String> listState;

                    @Override
                    public String map(String s) throws Exception {
                        listState.add(s);
                        StringBuilder sb = new StringBuilder();
                        for (String s1 : listState.get()) {
                            sb.append(s1);
                        }
                        return sb.toString();
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("strings", String.class);

                        // TTL配置
                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(4)) // 过期时间
                                .useProcessingTime() // 默认也是ProcessingTime时间语义

                                // .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // ttl重置刷新策略：数据被创建或者被写入更新，ttl重新计时
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 数据只要被读取或者写入更新，ttl重新计时

                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 数据状态可见性：如果状态过期但是还未清理，不让用户可见
                                // .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // 如果状态过期但是还未清理，让用户看见

                                // .cleanupFullSnapshot() // 全量快照时清理策略
                                .cleanupIncrementally(1000, true)
                                // .cleanupInRocksdbCompactFilter(1000)

                                .disableCleanupInBackground() // 禁用默认后台清理策略
                                .build();

                        // 开启ttl
                        descriptor.enableTimeToLive(ttlConfig);
                        listState = getRuntimeContext().getListState(descriptor);
                    }
                })
                .print();


        env.execute();
    }
}

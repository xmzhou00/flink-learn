package com.flinkcore.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author:xmzhou
 * @Date: 2022/6/5 13:53
 * @Description: operator state测试
 */
public class OperatorStateTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);

        // 开启checkpoint机制
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // 开启快照后，需要指定快照持久化存储的位置
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/flink_checkpoint");

        // 开启task级别的故障自动恢复 failover
        // 默认：不会failover 如果一个task出现错误，整个job就失败了
        // env.setRestartStrategy(RestartStrategies.noRestart());

        // 使用固定重启上限和重启时间间隔  重启三次，重启间隔1秒   如果失败次数超过三次，整个job就失败
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));


        env.socketTextStream("localhost", 9999)
                .map(new MyOperatorStateFunction())
                .print();

        env.execute("operatorState");
    }
}

// 要想使用OperatorState 需要开启checkpoint
class MyOperatorStateFunction extends RichMapFunction<String, String> implements CheckpointedFunction {
    // 注意：operatorState与程序的并行度是一一对应的，也就是这么说:如果一个task有多少个subtask，那么就有多少个operatorstate
    private ListState<String> listState;

    @Override
    public String map(String s) throws Exception {
        /**
         * 故意埋一个异常，来测试task级别的自动容错
         */
        if ("x".equals(s)) {
            throw new RuntimeException("task故障");
        }
        // 将数据存入到状态中
        listState.add(s);
        Iterable<String> strings = listState.get();
        StringBuilder stringBuilder = new StringBuilder();
        for (String string : strings) {
            stringBuilder.append(string);
        }
        return stringBuilder.toString();
    }

    /**
     * 进行 checkpoint 时会调用 snapshotState()
     *
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    /**
     * 算子任务（task）再启动时，会调用下此方法，来初始化状态
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 从context中获取一个算子状态存储器
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("strings", String.class);
        // 在task失败后，task自动重启时，会自动加载最近一次快照的状态数据
        // 如果时job重启，不会自动加载状态数据，此时就需要结合savepoint来实现
        listState = operatorStateStore.getListState(listStateDescriptor);

        /**
         * unionListState 和普通 ListState的区别：
         * unionListState的快照存储数据，在系统重启后，list数据的重分配模式为： 广播模式； 在每个subtask上都拥有一份完整的数据
         * ListState的快照存储数据，在系统重启后，list数据的重分配模式为： round-robin； 轮询平均分配
         */
        //ListState<String> unionListState = operatorStateStore.getUnionListState(stateDescriptor);

    }
}
package com.flinkcore.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Author:xmzhou
 * @Date: 2022/6/8 20:45
 * @Description: checkpoint相关配置
 */
public class CheckpointApp {
    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        /* ****************************************checkpoint配置**************************************************** */

        /*
         * 默认情况下 checkpoint是禁用的。通过调用
         */
        // 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(1000);

        // 设置模式为精确一次（默认）
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置两次ck之间的最小时间间隔，用于防止checkpoint过多地占用系统资源
        checkpointConfig.setMinPauseBetweenCheckpoints(2000);
        // 设置快照存储位置
        checkpointConfig.setCheckpointStorage("hdfs://hdp01:9090/ckp");
        // 设置checkpoint对齐的超时时长
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(3));
        // 设置非对齐模式下，再job恢复时让各个算子自动抛弃飞行中的数据
        checkpointConfig.setCheckpointIdOfIgnoredInFlightData(5);
        // 设置job cancel后，是否保存最后一次checkpoint的数据
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 是否强制使用 非对齐的checkpoint
        checkpointConfig.setForceUnalignedCheckpoints(false);
        // 设置一个算子在一次checkpoint执行过程中的总耗费时长超时上限
        checkpointConfig.setCheckpointTimeout(3000);
        // 设置允许checkpoint失败的最大次数
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        /* ****************************************statebackend**************************************************** */

        env.setStateBackend(new EmbeddedRocksDBStateBackend());


        /* ****************************************task失败重启配置**************************************************** */

        RestartStrategies.RestartStrategyConfiguration restartStrategy = null;
        /**
         * 固定重启
         * restartAttempts:重启次数
         * delayBetweenAttempts: 两次重启之间的延迟间隔
         */
        restartStrategy = RestartStrategies.fixedDelayRestart(3, 2000);

        // 失败不重启 一个task失败，整个job就失败
        restartStrategy = RestartStrategies.noRestart();

        /* *
         *  本策略：故障越频繁，两次重启间的惩罚间隔就越长
         *
         *  initialBackoff 重启间隔惩罚时长的初始值 ： 1s
         *  maxBackoff 重启间隔最大惩罚时长 : 60s
         *  backoffMultiplier 重启间隔时长的惩罚倍数: 2（ 每多故障一次，重启延迟惩罚就在 上一次的惩罚时长上 * 倍数）
         *  resetBackoffThreshold 重置惩罚时长的平稳运行时长阈值（平稳运行达到这个阈值后，如果再故障，则故障重启延迟时间重置为了初始值：1s）
         *  jitterFactor 取一个随机数来加在重启时间点上，让每次重启的时间点呈现一定随机性
         *     job1: 9.51   9.53+2*0.1    9.57   ......
         *     job2: 9.51   9.53+2*0.15   9.57   ......
         *     job3: 9.51   9.53+2*0.8    9.57   ......
         */
        restartStrategy = RestartStrategies.exponentialDelayRestart(Time.seconds(1),Time.seconds(60),2.0,Time.hours(1),1.0);

        /* *
         *  failureRate : 在指定时长内的最大失败次数
         *  failureInterval 指定的衡量时长
         *  delayInterval 两次重启之间的时间间隔
         */
        restartStrategy = RestartStrategies.failureRateRestart(5,Time.hours(1),Time.seconds(5));

        /* *
         *  本策略就是退回到配置文件所配置的策略
         *  常用于自定义 RestartStrategy
         *  用户自定义了重启策略类，常常配置在 flink-conf.yaml 文件中
         */
        restartStrategy = RestartStrategies.fallBackRestart();

        // 在环境中设置指定的重启策略
        env.setRestartStrategy(restartStrategy);


    }
}

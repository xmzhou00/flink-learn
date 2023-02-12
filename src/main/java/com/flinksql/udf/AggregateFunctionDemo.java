package com.flinksql.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/6 9:56
 * @Description: 自定义聚合函数
 */
public class AggregateFunctionDemo {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        Table table = tenv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("uid", DataTypes.INT()),
                        DataTypes.FIELD("gender", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                ),
                Row.of(1, "male", 80),
                Row.of(2, "female", 100),
                Row.of(3, "male", 80)
        );
        tenv.createTemporaryView("t", table);
        tenv.createTemporarySystemFunction("myAvg",WeightedAvg.class);

        tenv.executeSql("select gender,myAvg(score) from t group by gender").print();

    }

    public static class WeightedAvgAccum {
        public double sum = 0.0;
        public int count = 0;
    }

    public static class WeightedAvg extends AggregateFunction<Double, WeightedAvgAccum> {
        /**
         * 创建累加器
         *
         * @return
         */
        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        /**
         * 输出值
         * @param weightedAvgAccum
         * @return
         */
        @Override
        public Double getValue(WeightedAvgAccum weightedAvgAccum) {
            return weightedAvgAccum.sum/weightedAvgAccum.count;
        }

        /**
         * 累加
         * @param acc
         * @param score
         */
        public void accumulate(WeightedAvgAccum acc, Double score) {
            acc.count += 1;
            acc.sum += score;
        }

    }
}

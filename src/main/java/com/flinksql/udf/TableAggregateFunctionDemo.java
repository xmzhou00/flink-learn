package com.flinksql.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/6 12:16
 * @Description: 表聚合函数
 * 自定义表聚合函数示例
 * 什么叫做表聚合函数：
 * 1,male,zs,88
 * 2,male,bb,99
 * 3,male,cc,76
 * 4,female,dd,78
 * 5,female,ee,92
 * 6,female,ff,86
 * <p>
 * -- 求每种性别中，分数最高的两个成绩
 * -- 常规写法
 * SELECT
 * *
 * FROM
 * (
 * SELECT
 * gender,
 * score,
 * row_number() over(partition by gender order by score desc) as rn
 * FROM  t
 * )
 * where rn<=2
 * <p>
 * <p>
 * -- 如果有一种聚合函数，能在分组聚合的模式中，对每组数据输出多行多列聚合结果
 * SELECT
 * gender,
 * top2(score)
 * from t
 * group by gender
 * <p>
 * male,88
 * male,99
 * female,92
 * female,86
 */
public class TableAggregateFunctionDemo {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = tenv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("gender", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())),
                Row.of(1, "male", 67),
                Row.of(2, "male", 88),
                Row.of(3, "male", 98),
                Row.of(4, "female", 99),
                Row.of(5, "female", 84),
                Row.of(6, "female", 89)
        );
        tenv.createTemporaryView("t", table);

//        tenv.executeSql("select gender,max(score) from t group by gender").print();

// 用一个聚合函数直接求每种性别的最高的两个成绩
        // 表聚合函数只能使用table api方式
        table.groupBy($("gender"))
                .flatAggregate(call(MyTop2.class,$("score")))
                .select($("gender"),$("score_top"),$("rank"))
                .execute().print();
    }

    public static class myAcc {
        public double first = Double.MIN_VALUE;
        public double second = Double.MIN_VALUE;
    }

    @FunctionHint(output = @DataTypeHint("ROW<score_top DOUBLE, rank INT>"))
    public static class MyTop2 extends TableAggregateFunction<Row, myAcc> {

        /**
         * 创建累加器
         *
         * @return
         */
        @Override
        public myAcc createAccumulator() {
            return new myAcc();
        }

        /**
         * 累加更新逻辑
         *
         * @param acc
         * @param score
         */
        public void accumulate(myAcc acc, Double score) {
            if (score > acc.first) {
                acc.second = acc.first;
                acc.first = score;
            } else if (score > acc.second) {
                acc.second = score;
            }
        }

        public void merge(myAcc acc, Iterable<myAcc> iterable) {
            for (myAcc myAcc : iterable) {
                accumulate(acc, myAcc.first);
                accumulate(acc, myAcc.second);
            }
        }

        public void emitValue(myAcc acc, Collector<Row> out) {
            if (acc.first != Double.MIN_VALUE) {
                out.collect(Row.of(acc.first, 1));
            }
            if (acc.second != Double.MIN_VALUE) {
                out.collect(Row.of(acc.second, 2));
            }
        }
    }

}

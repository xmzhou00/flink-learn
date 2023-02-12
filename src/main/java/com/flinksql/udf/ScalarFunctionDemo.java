package com.flinksql.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/6 9:17
 * @Description: 自定义标量函数
 */
public class ScalarFunctionDemo {
    public static void main(String[] args) {
        TableEnvironment tEnv  = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        Table table = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING())),
                Row.of("aaa"),
                Row.of("bbb"),
                Row.of("ccc")
        );

        tEnv.createTemporaryView("u",table);

        // 1. table api中不用注册函数，直接可以使用
//       table.select(call(GetFirstWord.class,$("name"))).execute().print();


//       2. 注册函数
            tEnv.createTemporarySystemFunction("getFirst", GetFirstWord.class);


            tEnv.executeSql("select getFirst(name),name from u").print();

    }

    public static class GetFirstWord extends ScalarFunction{

        public String eval(String s){
            return s.substring(0,1);
        }
    }
}

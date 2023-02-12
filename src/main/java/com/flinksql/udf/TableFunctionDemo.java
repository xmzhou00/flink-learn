package com.flinksql.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author: xianmingZhou
 * @Date: 2022/8/6 9:35
 * @Description: 表生成函数
 */
public class TableFunctionDemo {
    public static void main(String[] args) {
        TableEnvironment tenv     = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = tenv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("phone_numbers", DataTypes.STRING())),
                Row.of(1, "zs", "13888,137,1354455"),
                Row.of(2, "bb",  "1366688,1374,132224455")
        );
        tenv.createTemporaryView("t",table);

        // 注册函数
        tenv.createTemporarySystemFunction("mysplit",MySplit.class);

        tenv.executeSql("select * from t,lateral table(mysplit(phone_numbers,',')) as t1(word,length)").print();
        tenv.executeSql("select * from t left join lateral table(mysplit(phone_numbers,',')) as t1(word,length) on true").print();



    }
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class MySplit extends TableFunction<Row>{
        public void eval(String str,String delimiter){
            for (String s : str.split(delimiter)) {
                collect(Row.of(s,s.length()));
            }
        }
    }
}

package com.flinkcore.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author:xmzhou
 * @Date: 2022/7/30 15:05
 * @Description:
 */
public class JdbcSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SinkFunction<String> jdbcSink = JdbcSink.sink(
                "insert into t_test (line) values(?)",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
                        preparedStatement.setString(1, s);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/flink")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        env
                .socketTextStream("localhost", 9999)
                .addSink(jdbcSink);

        env.execute();

    }
}

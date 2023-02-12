package com.flinkcore.sink;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author:xmzhou
 * @Date: 2022/7/30 15:05
 * @Description:
 */
public class JdbcExactlyOnceSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 注意：ExactlyOnceSink需要开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        SinkFunction<String> exactlyOnceSink = JdbcSink.exactlyOnceSink(
                "insert into t_test (line) values(?)",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
                        preparedStatement.setString(1, s);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(0)
                        .withBatchSize(1)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        // mysql不支持同一个连接上存在并行的多个事务，必须把该参数设置为true
                        .withTransactionPerConnection(true)
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        // XADataSource就是jdbc连接，它支持分布式事务的连接
                        MysqlXADataSource dataSource = new MysqlXADataSource();
                        dataSource.setURL("jdbc:mysql://localhost:3306/flink");
                        dataSource.setUser("root");
                        dataSource.setPassword("123456");
                        return dataSource;
                    }
                }
        );


        env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        System.out.println(value);
                        return value;
                    }
                })
                .addSink(exactlyOnceSink);


        env.execute();

    }
}

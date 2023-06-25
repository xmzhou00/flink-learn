package com.flinkcore.datalake.reader;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * Flink Iceberg Streaming Mode Reader
 */
public class FlinkIcebergStreamingReader {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableLoader tableLoader = TableLoader.fromHadoopTable("D:\\iceberg\\tmp\\default\\t");
        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .build();

        stream.print();

        env.execute("Iceberg Reader");
    }
}
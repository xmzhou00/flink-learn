package com.flinkcore.datalake.writer;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

import java.util.List;

/**
 * 项目描述：主要用于观察Flink流式写入Iceberg中，本地文件系统中小文件的变化
 */
public class IcebergSinkByNetcat {

    private static final String DATABASE = "default";
    private static final String TABLE = "t";

//    public static final RowType ROW_TYPE = (RowType) FLINK_SCHEMA.toRowDataType().getLogicalType();

    private static final boolean partitioned = false;

//    private static BoundedTestSource<Row> createBoundedSource(List<Row> rows) {
//        return new BoundedTestSource<>(rows.toArray(new Row[0]));
//    }

    public static void main(String[] args) throws Throwable {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration conf = new Configuration();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // must enable
        env.enableCheckpointing(1000);
        env.setParallelism(1);

//        final HadoopCatalogResource catalogResource =
//                new HadoopCatalogResource(DATABASE, TABLE);

        DataStream<RowData> stream = env
                .socketTextStream("192.168.10.156", 9999)
                .map(new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String s) throws Exception {
                        String[] fields = s.split(",");
                        GenericRowData data = new GenericRowData(2);
                        data.setField(0, fields[0]);
                        data.setField(1, fields[1]);
                        return data;
                    }
                });

//        stream.print();
        Configuration conf = new Configuration();
        TableLoader tableLoader = TableLoader.fromHadoopTable("D:\\iceberg\\tmp", conf);

//
//        Schema schema =
//                new Schema(
//                        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
//                        Types.NestedField.optional(2, "data", Types.StringType.get()));
//
//        Table table = catalogResource
//                .catalog()
//                .createTable(
//                        TableIdentifier.of(DATABASE, TABLE),
//                        schema,
//                        partitioned
//                                ? PartitionSpec.builderFor(schema).identity("data").build()
//                                : PartitionSpec.unpartitioned(),
//                        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.AVRO.name()));
//
//        TableLoader tableLoader = catalogResource.tableLoader();

        FlinkSink.forRowData(stream)
                .tableLoader(tableLoader)
//                .writeParallelism(1)
                .append();

        env.execute("Test Iceberg DataStream");
    }
}

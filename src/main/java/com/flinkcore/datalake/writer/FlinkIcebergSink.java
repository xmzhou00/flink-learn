//package com.flinkcore.datalake.writer;
//
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.api.TableSchema;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.data.util.DataFormatConverters;
//import org.apache.flink.table.types.logical.RowType;
//import org.apache.flink.types.Row;
//import org.apache.iceberg.*;
//import org.apache.iceberg.catalog.TableIdentifier;
//import org.apache.iceberg.flink.TableLoader;
//import org.apache.iceberg.flink.sink.FlinkSink;
//import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
//import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
//import org.apache.iceberg.relocated.com.google.common.collect.Lists;
//import org.apache.iceberg.types.Types;
//
//import java.util.List;
//
///**
// * Flink DataStream 写入 Iceberg
// * flink version: 1.17
// * Iceberg version: 1.3.0
// *
// */
//public class FlinkIcebergSink {
//    public static final TableSchema FLINK_SCHEMA =
//            TableSchema.builder().field("id", DataTypes.INT()).field("data", DataTypes.STRING()).build();
//    protected static final DataFormatConverters.RowConverter CONVERTER =
//            new DataFormatConverters.RowConverter(FLINK_SCHEMA.getFieldDataTypes());
//
//    protected static final TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(FLINK_SCHEMA.getFieldTypes());
//
//    private static final String DATABASE = "default";
//    private static final String TABLE = "t";
//
//    public static final RowType ROW_TYPE = (RowType) FLINK_SCHEMA.toRowDataType().getLogicalType();
//
//    private static final boolean partitioned = false;
//
//    private static BoundedTestSource<Row> createBoundedSource(List<Row> rows) {
//        return new BoundedTestSource<>(rows.toArray(new Row[0]));
//    }
//
//    public static void main(String[] args) throws Throwable {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // must enable
//        env.enableCheckpointing(1000);
//
//        final HadoopCatalogResource catalogResource =
//                new HadoopCatalogResource(DATABASE, TABLE);
//
//        List<Row> rows = Lists.newArrayList(Row.of(4, "iceberg"), Row.of(5, "delta"), Row.of(6, "hudi"));
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
//
//        DataStream<RowData> dataStream =
//                env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
//                        .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(ROW_TYPE));
//        FlinkSink.forRowData(dataStream)
//                .table(table)
//                .tableLoader(tableLoader)
//                .writeParallelism(1)
//                .append();
//
//        env.execute("Test Iceberg DataStream");
//    }
//}

package com.flinkcore.datalake.writer;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.File;

public class HadoopCatalogResource {
    protected final String database;
    protected final String tableName;

    protected Catalog catalog;
    protected CatalogLoader catalogLoader;
    protected String warehouse;
    protected TableLoader tableLoader;

    public HadoopCatalogResource(String database, String tableName) throws Throwable {
        this.database = database;
        this.tableName = tableName;
        before();
    }

    protected void before() throws Throwable {
        File warehouseFile = new File("D:/iceberg/tmp");
        this.warehouse = "file:///" + warehouseFile;
        this.catalogLoader =
                CatalogLoader.hadoop(
                        "hadoop",
                        new Configuration(),
                        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse));
        this.catalog = catalogLoader.loadCatalog();
        this.tableLoader =
                TableLoader.fromCatalog(catalogLoader, TableIdentifier.of(database, tableName));
    }

    protected void after() {
        try {
            catalog.dropTable(TableIdentifier.of(database, tableName));
            ((HadoopCatalog) catalog).close();
            tableLoader.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close catalog resource");
        }
    }

    public TableLoader tableLoader() {
        return tableLoader;
    }

    public Catalog catalog() {
        return catalog;
    }

    public CatalogLoader catalogLoader() {
        return catalogLoader;
    }

    public String warehouse() {
        return warehouse;
    }
}
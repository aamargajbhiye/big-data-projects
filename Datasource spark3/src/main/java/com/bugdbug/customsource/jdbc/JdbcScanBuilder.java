package com.bugdbug.customsource.jdbc;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

public class JdbcScanBuilder implements ScanBuilder {
    private final StructType schema;
    private final JdbcParams jdbcParams;

    public JdbcScanBuilder(StructType schema, JdbcParams jdbcParams) {
        this.schema = schema;
        this.jdbcParams = jdbcParams;
    }

    @Override
    public Scan build() {
        return new JdbcScan(schema, jdbcParams);
    }
}

package com.bugdbug.customsource.jdbc.read;

import com.bugdbug.customsource.jdbc.JdbcParams;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class JdbcScan implements Scan {
    private final StructType schema;
    private final JdbcParams jdbcParams;

    public JdbcScan(StructType schema, JdbcParams jdbcParams) {
        this.schema = schema;
        this.jdbcParams = jdbcParams;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return jdbcParams.getTableName();
    }

    @Override
    public Batch toBatch() {
        return new JdbcBatch(schema, jdbcParams);
    }
}

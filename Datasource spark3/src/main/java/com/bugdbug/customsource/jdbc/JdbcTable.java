package com.bugdbug.customsource.jdbc;

import com.bugdbug.customsource.jdbc.read.JdbcScanBuilder;
import com.bugdbug.customsource.jdbc.write.JdbcWriteBuilder;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JdbcTable implements SupportsRead, SupportsWrite {

    private final StructType schema;
    private final JdbcParams jdbcParams;
    private Set<TableCapability> capabilities;

    public JdbcTable(StructType schema, JdbcParams jdbcParams) {
        this.schema = schema;
        this.jdbcParams = jdbcParams;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new JdbcScanBuilder(schema, jdbcParams);
    }

    @Override
    public String name() {
        return jdbcParams.getTableName();
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.BATCH_READ);
            capabilities.add(TableCapability.BATCH_WRITE);
        }
        return capabilities;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
        return new JdbcWriteBuilder(logicalWriteInfo);
    }
}

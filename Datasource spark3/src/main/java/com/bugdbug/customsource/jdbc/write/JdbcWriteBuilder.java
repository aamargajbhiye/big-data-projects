package com.bugdbug.customsource.jdbc.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class JdbcWriteBuilder implements WriteBuilder {

    private final LogicalWriteInfo logicalWriteInfo;

    public JdbcWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
        this.logicalWriteInfo = logicalWriteInfo;
    }

    @Override
    public BatchWrite buildForBatch() {
        return new JdbcBatchWrite(this.logicalWriteInfo);
    }
}

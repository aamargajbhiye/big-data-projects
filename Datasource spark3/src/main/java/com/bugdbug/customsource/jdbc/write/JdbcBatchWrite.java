package com.bugdbug.customsource.jdbc.write;

import com.bugdbug.customsource.jdbc.JdbcParams;
import com.bugdbug.customsource.jdbc.utils.JdbcUtil;
import com.bugdbug.customsource.jdbc.utils.Util;
import org.apache.spark.sql.connector.write.*;

public class JdbcBatchWrite implements BatchWrite {
    private final LogicalWriteInfo logicalWriteInfo;
    private final JdbcParams originalJdbcParams;
    private final JdbcParams modifiedjdbcParams;

    public JdbcBatchWrite(LogicalWriteInfo logicalWriteInfo) {
        this.logicalWriteInfo = logicalWriteInfo;
        this.originalJdbcParams = Util.extractOptions(logicalWriteInfo.options());
        // create temp table
        this.modifiedjdbcParams = this.originalJdbcParams.buildWith(this.originalJdbcParams.getTableName() + "_temp");
        JdbcUtil.createTable(logicalWriteInfo.schema(), this.modifiedjdbcParams);
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return new JdbcDataWriterFactory(logicalWriteInfo.schema(), modifiedjdbcParams);
    }

    @Override
    public void commit(WriterCommitMessage[] writerCommitMessages) {
        JdbcUtil.dropTable(originalJdbcParams);
        JdbcUtil.renameTable(this.modifiedjdbcParams.getTableName(),
                originalJdbcParams.getTableName(), originalJdbcParams);
    }

    @Override
    public void abort(WriterCommitMessage[] writerCommitMessages) {
        JdbcUtil.dropTable(modifiedjdbcParams);
    }

    public boolean useCommitCoordinator() {
        return true;
    }
}

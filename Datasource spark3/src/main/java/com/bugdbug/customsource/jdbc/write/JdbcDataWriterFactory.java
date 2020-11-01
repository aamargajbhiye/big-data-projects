package com.bugdbug.customsource.jdbc.write;

import com.bugdbug.customsource.jdbc.JdbcParams;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class JdbcDataWriterFactory implements DataWriterFactory {
    private final StructType schema;
    private final JdbcParams jdbcParams;

    public JdbcDataWriterFactory(StructType schema, JdbcParams jdbcParams) {
        this.schema = schema;
        this.jdbcParams = jdbcParams;

    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new JdbcDataWriter(partitionId, taskId, schema, jdbcParams);
    }
}

package com.bugdbug.customsource.jdbc.write;

import com.bugdbug.customsource.jdbc.JdbcParams;
import com.bugdbug.customsource.jdbc.Setters;
import com.bugdbug.customsource.jdbc.TriConsumer;
import com.bugdbug.customsource.jdbc.utils.JdbcUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class JdbcDataWriter implements DataWriter<InternalRow> {
    private final int partitionId;
    private final long taskId;
    private PreparedStatement preparedStatement;
    private Connection connection;
    private final List<TriConsumer<PreparedStatement, Integer, InternalRow>> setters;

    public JdbcDataWriter(int partitionId, long taskId,
                          StructType schema, JdbcParams jdbcParams) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.setters = Setters.getSetters(schema);
        initializePreparedStatement(schema, jdbcParams);
    }

    @Override
    public void write(InternalRow internalRow) {
        writeRow(internalRow);
    }

    private void writeRow(InternalRow internalRow) {
        for (int i = 0; i < setters.size(); i++) {
            setters.get(i).apply(preparedStatement, i + 1, internalRow);
        }
    }

    @Override
    public WriterCommitMessage commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new WriterCommitMessageImpl(partitionId, taskId);
    }

    @Override
    public void abort() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void initializePreparedStatement(StructType schema, JdbcParams jdbcParams) {
        try {
            this.connection = JdbcUtil.getConnection(jdbcParams);
            connection.setAutoCommit(false);
            String insertQuery = JdbcUtil.createInsertQuery(schema, jdbcParams);
            this.preparedStatement = connection.prepareStatement(insertQuery);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }
}

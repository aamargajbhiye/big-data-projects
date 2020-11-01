package com.bugdbug.customsource.jdbc.write;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Objects;

public class WriterCommitMessageImpl implements WriterCommitMessage {

    private final int partitionId;
    private final long taskId;

    public WriterCommitMessageImpl(int partitionId, long taskId){

        this.partitionId = partitionId;
        this.taskId = taskId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WriterCommitMessageImpl)) return false;
        WriterCommitMessageImpl that = (WriterCommitMessageImpl) o;
        return partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId);
    }


    @Override
    public String toString() {
        return "WriterCommitMessageImpl{" +
                "partitionId=" + partitionId +
                ", taskId=" + taskId +
                '}';
    }

    public long getTaskId() {
        return taskId;
    }
}

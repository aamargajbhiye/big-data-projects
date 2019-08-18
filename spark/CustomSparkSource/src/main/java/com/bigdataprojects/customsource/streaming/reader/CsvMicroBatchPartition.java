package com.bigdataprojects.customsource.streaming.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.List;

class CsvMicroBatchPartition implements InputPartition<InternalRow> {

    private List<Object[]> rowChunk;

    CsvMicroBatchPartition(List<Object[]> rowChunk) {

        this.rowChunk = rowChunk;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return new CsvMicroBatchPartitionReader(rowChunk);
    }
}

class CsvMicroBatchPartitionReader implements InputPartitionReader<InternalRow> {

    private List<Object[]> rowChunk;
    private int counter;

    CsvMicroBatchPartitionReader(List<Object[]> rowChunk) {

        this.rowChunk = rowChunk;
        this.counter = 0;

    }

    @Override
    public boolean next() {
        return counter < rowChunk.size();
    }

    @Override
    public InternalRow get() {

        Object[] row = rowChunk.get(counter++);

        return InternalRow.apply(JavaConversions.asScalaBuffer(Arrays.asList(row)).seq());
    }

    @Override
    public void close() {

    }
}

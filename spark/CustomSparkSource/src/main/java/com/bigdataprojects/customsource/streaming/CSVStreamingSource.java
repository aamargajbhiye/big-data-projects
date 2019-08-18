package com.bigdataprojects.customsource.streaming;

import com.bigdataprojects.customsource.streaming.reader.CSVMicroBatchReader;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

/**
 * This class is custom implementation of streaming interfaces for Apache Spark
 */
public class CSVStreamingSource implements DataSourceV2, MicroBatchReadSupport {

    public CSVStreamingSource() {

    }

    @Override
    public MicroBatchReader createMicroBatchReader(Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
        return new CSVMicroBatchReader(options.get("filepath").get());
    }
}

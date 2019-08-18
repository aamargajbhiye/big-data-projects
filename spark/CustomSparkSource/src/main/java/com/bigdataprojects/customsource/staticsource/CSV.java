package com.bigdataprojects.customsource.staticsource;

import com.bigdataprojects.customsource.staticsource.reader.CSVDataSourceReader;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

/***
 * Custom spark data source
 */
public class CSV implements DataSourceV2, ReadSupport, WriteSupport {

    public CSV() {

    }

    @Override
    public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
        return new CSVDataSourceReader(options.get("filepath").get(),schema);
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return  new CSVDataSourceReader(options.get("filepath").get());
    }

    @Override
    public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
        return Optional.empty();
    }
}

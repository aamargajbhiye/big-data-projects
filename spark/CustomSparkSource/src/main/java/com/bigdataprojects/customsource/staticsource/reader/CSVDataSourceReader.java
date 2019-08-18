package com.bigdataprojects.customsource.staticsource.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * This class defines the data reader using given schema {@link StructType}, {@link Filter} and pushed columns
 */
public class CSVDataSourceReader implements DataSourceReader, SupportsPushDownFilters,
        SupportsPushDownRequiredColumns {

    private final String filePath;
    private final StructType schema;

    public CSVDataSourceReader(String filePath, StructType schema) {

        this.filePath = filePath;
        this.schema = schema;
    }

    public CSVDataSourceReader(String filepath) {
        this.filePath = filepath;

        StructField[] structFields = new StructField[]{
                new StructField("Region", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Country", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Item Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Sales Channel", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order Priority", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order ID", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Ship Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Units Sold", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Unit Price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Unit Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Revenue", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Profit", DataTypes.DoubleType, true, Metadata.empty())
        };
        this.schema = new StructType(structFields);
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        return Arrays.asList(new CSVInputPartition(filePath));
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        return new Filter[0];
    }

    @Override
    public Filter[] pushedFilters() {
        return new Filter[0];
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
    }
}

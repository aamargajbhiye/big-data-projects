package com.bugdbug.customsource.csv;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class CSV implements TableProvider {
    public CSV() {

    }
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new CsvTable(schema,properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}

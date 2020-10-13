package com.bugdbug.customsource.jdbc;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.sql.SQLException;
import java.util.Map;

public class JDBC implements TableProvider {
    private JdbcParams jdbcParams;

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        this.jdbcParams = Utils.extractOptions(options);
        try {
            return Utils.getSchema(this.jdbcParams);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new JdbcTable(schema, this.jdbcParams);
    }
}

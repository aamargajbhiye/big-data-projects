package com.bugdbug.customsource.jdbc;

import org.apache.spark.sql.types.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Utils {

    public static StructType getSchema(JdbcParams jdbcParams) throws SQLException, ClassNotFoundException {
        ResultSetMetaData resultSetMetaData = resultSetMetaData(jdbcParams);
        StructField[] structFields = new StructField[resultSetMetaData.getColumnCount()];
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            int columnType = resultSetMetaData.getColumnType(i);
            int precision = resultSetMetaData.getPrecision(i);
            int scale = resultSetMetaData.getScale(i);
            DataType dataType = getDataType(columnType, precision, scale);
            StructField structField =
                    new StructField(columnName, dataType, true, Metadata.empty());
            structFields[i - 1] = structField;
        }
        return new StructType(structFields);
    }

    public static Connection getConnection(JdbcParams jdbcParams)
            throws ClassNotFoundException, SQLException {
        Class.forName(jdbcParams.getJdbcDriver());
        System.out.println("Connecting to database...");
        Connection connection = DriverManager.getConnection(jdbcParams.getJdbcUrl(),
                jdbcParams.getUserName(), jdbcParams.getPassword());

        return connection;
    }

    public static JdbcParams extractOptions(Map<String, String> options) {
        JdbcParams jdbcParams = new JdbcParams.JdbcParamsBuilder()
                .setTableName(options.get(Constants.TABLE_NAME))
                .setJdbcUrl(options.get(Constants.JDBC_URL))
                .setUserName(options.get(Constants.USER))
                .setPassword(options.get(Constants.PASSWORD))
                .setJdbcDriver(options.get(Constants.JDBC_DRIVER))
                .setPartitioningColumn(options.get(Constants.PARTITIONING_COLUMN))
                .setNumPartitions(
                        options.get(Constants.NUM_PARTITIONS) != null
                                ? Integer.parseInt(options.get(Constants.NUM_PARTITIONS))
                                : 1)
                .setLocalityInfo(getLocalityInfo(options))
                .build();
        return jdbcParams;
    }

    private static List<String> getLocalityInfo(Map<String, String> options) {
        String localityInfo = options.get(Constants.LOCALITY_INFO);
        String[] localityNodes = localityInfo.split(",");
        return Arrays.asList(localityNodes);
    }

    public static ResultSetMetaData resultSetMetaData(JdbcParams jdbcParams) throws SQLException, ClassNotFoundException {
        String query = "Select * from " + jdbcParams.getTableName() + " where 1=0";
        Connection connection = getConnection(jdbcParams);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        return resultSet.getMetaData();
    }

    private static DataType getDataType(int sqlType, int precision, int scale) {
        switch (sqlType) {
            case Types.INTEGER:
                return DataTypes.IntegerType;
            case Types.DOUBLE:
                return DataTypes.DoubleType;
            case Types.FLOAT:
                return DataTypes.FloatType;
            case Types.NUMERIC:
                return DataTypes.createDecimalType(precision, scale);
            case Types.VARCHAR:
            case Types.CHAR:
            default:
                return DataTypes.StringType;
        }
    }
}

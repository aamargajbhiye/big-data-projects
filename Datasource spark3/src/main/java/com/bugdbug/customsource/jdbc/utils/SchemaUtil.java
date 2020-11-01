package com.bugdbug.customsource.jdbc.utils;

import com.bugdbug.customsource.jdbc.JdbcParams;
import org.apache.spark.sql.types.*;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class SchemaUtil {

    public static DataType getDataType(int sqlType, int precision, int scale) {
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


    public static String getSqlDataType(DataType dataType) {
        if (DataTypes.IntegerType.equals(dataType))
            return "INT";
        else if (DataTypes.FloatType.equals(dataType))
            return "FLOAT";
        else if (DataTypes.DoubleType.equals(dataType))
            return "DOUBLE";
        else if (DataTypes.StringType.equals(dataType))
            return "VARCHAR";
        else if (dataType.typeName().startsWith("Decimal")) {
            return "DECIMAL";
        } else
            return "VARCHAR";
    }

    public static StructType getSchema(JdbcParams jdbcParams) throws SQLException, ClassNotFoundException {
        ResultSetMetaData resultSetMetaData = JdbcUtil.resultSetMetaData(jdbcParams);
        StructField[] structFields = new StructField[resultSetMetaData.getColumnCount()];
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            int columnType = resultSetMetaData.getColumnType(i);
            int precision = resultSetMetaData.getPrecision(i);
            int scale = resultSetMetaData.getScale(i);
            DataType dataType = SchemaUtil.getDataType(columnType, precision, scale);
            StructField structField =
                    new StructField(columnName, dataType, true, Metadata.empty());
            structFields[i - 1] = structField;
        }
        return new StructType(structFields);
    }
}

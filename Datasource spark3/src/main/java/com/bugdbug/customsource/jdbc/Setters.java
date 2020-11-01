package com.bugdbug.customsource.jdbc;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Setters {

    public static List<TriConsumer<PreparedStatement, Integer, InternalRow>> getSetters(StructType schema) {
        StructField[] fields = schema.fields();
        List<TriConsumer<PreparedStatement, Integer, InternalRow>> setters = new ArrayList<>(fields.length);
        Arrays.stream(fields).forEach(field -> {
            if (field.dataType().equals(DataTypes.StringType)) {
                setters.add(stringSetter);
            } else if (field.dataType().equals(DataTypes.IntegerType))
                setters.add(intSetter);
            else if (field.dataType().equals(DataTypes.DoubleType))
                setters.add(doubleSetter);
            else if (field.dataType().equals(DataTypes.FloatType))
                setters.add(floatSetter);
        });
        return setters;
    }


    public static TriConsumer<PreparedStatement, Integer, InternalRow> stringSetter =
            (preparedStatement, index, internalRow) -> {
                try {
                    preparedStatement.setString(index, internalRow.getString(index-1));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            };
    public static TriConsumer<PreparedStatement, Integer, InternalRow> doubleSetter =
            (preparedStatement, index, internalRow) -> {
                try {
                    preparedStatement.setDouble(index, internalRow.getDouble(index));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            };
    public static TriConsumer<PreparedStatement, Integer, InternalRow> intSetter =
            (preparedStatement, index, internalRow) -> {
                try {
                    preparedStatement.setInt(index, internalRow.getInt(index));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            };

    public static TriConsumer<PreparedStatement, Integer, InternalRow> floatSetter =
            (preparedStatement, index, internalRow) -> {
                try {
                    preparedStatement.setFloat(index, internalRow.getFloat(index));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            };

    public static TriConsumer<PreparedStatement, Integer, InternalRow> decimalSetter =
            (preparedStatement, index, internalRow) -> {
                try {
                    preparedStatement.setDouble(index, internalRow.getDouble(index));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            };
}

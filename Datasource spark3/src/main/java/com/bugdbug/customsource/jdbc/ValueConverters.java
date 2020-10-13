package com.bugdbug.customsource.jdbc;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ValueConverters {

    public static List<BiFunction> getConverters(StructType schema) {
        StructField[] fields = schema.fields();
        List<BiFunction> valueConverters = new ArrayList<>(fields.length);
        Arrays.stream(fields).forEach(field -> {
            if (field.dataType().equals(DataTypes.StringType)) {
                valueConverters.add(UTF8StringConverter);
            } else if (field.dataType().equals(DataTypes.IntegerType))
                valueConverters.add(IntConverter);
            else if (field.dataType().equals(DataTypes.DoubleType))
                valueConverters.add(DoubleConverter);
        });
        return valueConverters;
    }


    public static BiFunction<ResultSet, Integer, UTF8String> UTF8StringConverter =
            (resultSet, index) -> {
                try {
                    return UTF8String.fromString(resultSet.getString(index));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            };
    public static BiFunction<ResultSet, Integer, Double> DoubleConverter = (resultSet, index) -> {
        try {
            return resultSet.getDouble(index);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    };
    public static BiFunction<ResultSet, Integer, Integer> IntConverter = (resultSet, index) -> {
        try {
            return resultSet.getInt(index);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    };
}

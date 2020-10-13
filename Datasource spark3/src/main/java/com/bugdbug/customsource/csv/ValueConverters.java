package com.bugdbug.customsource.csv;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ValueConverters {

    public static List<Function> getConverters(StructType schema) {
        StructField[] fields = schema.fields();
        List<Function> valueConverters = new ArrayList<>(fields.length);
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


    public static Function<String, UTF8String> UTF8StringConverter = UTF8String::fromString;
    public static Function<String, Double> DoubleConverter = value -> value == null ? null : Double.parseDouble(value);
    public static Function<String, Integer> IntConverter = value -> value == null ? null : Integer.parseInt(value);

}

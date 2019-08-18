package com.bigdataprojects.customsource.staticsource;

import org.apache.spark.unsafe.types.UTF8String;

public interface ValueConverter<I, R> {

    R convertValue(String value);

    class IntegerConverter implements ValueConverter<String, Integer> {

        @Override
        public Integer convertValue(String value) {
            return value == null ? null : Integer.parseInt(value);
        }
    }

    class DoubleConverter implements ValueConverter<String, Double> {

        @Override
        public Double convertValue(String value) {
            return value == null ? null : Double.parseDouble(value);
        }
    }

    class UTF8StringConverter implements ValueConverter<String, UTF8String> {

        @Override
        public UTF8String convertValue(String value) {
            return UTF8String.fromString(value);
        }
    }

    class StringConverter implements ValueConverter<String, String> {

        @Override
        public String convertValue(String value) {
            return value;
        }
    }

}

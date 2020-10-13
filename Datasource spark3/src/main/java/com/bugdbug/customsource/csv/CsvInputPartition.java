package com.bugdbug.customsource.csv;

import org.apache.spark.sql.connector.read.InputPartition;

public class CsvInputPartition implements InputPartition {

    @Override
    public String[] preferredLocations() {
        return new String[0];
    }
}

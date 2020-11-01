package com.bugdbug.customsource.jdbc.read;

import org.apache.spark.sql.connector.read.InputPartition;

public class JdbcInputPartition implements InputPartition {
    private final Integer[] values;
    private final String hostAddress;

    public JdbcInputPartition(Integer[] values, String hostAddress) {
        this.values = values;
        this.hostAddress = hostAddress;
    }

    @Override
    public String[] preferredLocations() {
        return new String[]{hostAddress};
    }

    public Integer[] getValues() {
        return values;
    }
}

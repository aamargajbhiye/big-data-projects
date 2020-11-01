package com.bugdbug.customsource.jdbc.utils;

import com.bugdbug.customsource.jdbc.Constants;
import com.bugdbug.customsource.jdbc.JdbcParams;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Util {
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
        if (localityInfo == null)
            return null;
        String[] localityNodes = localityInfo.split(",");
        return Arrays.asList(localityNodes);
    }
}

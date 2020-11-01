package com.bugdbug.customsource.jdbc;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class JdbcParams implements Serializable {
    private final JdbcParamsBuilder builder;
    private String tableName;
    private String jdbcUrl;
    private String userName;
    private String password;
    private String jdbcDriver;
    private String partitioningColumn;
    private int numPartitions;
    private List<String> localityInfo;

    public JdbcParams(JdbcParamsBuilder builder) {
        this.tableName = builder.tableName;
        this.jdbcUrl = builder.jdbcUrl;
        this.userName = builder.userName;
        this.password = builder.password;
        this.jdbcDriver = builder.jdbcDriver;
        this.partitioningColumn = builder.partitioningColumn;
        this.numPartitions = builder.numPartitions;
        this.localityInfo = builder.localityInfo;
        this.builder = builder;
    }

    public String getPartitioningColumn() {
        return partitioningColumn;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public String getTableName() {
        return tableName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public List<String> getLocalityInfo() {
        return localityInfo;
    }

    public JdbcParams buildWith(String tableName) {
        return builder.setTableName(tableName).build();
    }

    public static class JdbcParamsBuilder implements Serializable{
        private String tableName;
        private String jdbcUrl;
        private String userName;
        private String password;
        private String jdbcDriver;
        private String partitioningColumn;
        private int numPartitions;
        private List<String> localityInfo;

        public JdbcParamsBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public JdbcParamsBuilder setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public JdbcParamsBuilder setUserName(String userName) {
            this.userName = userName;
            return this;
        }

        public JdbcParamsBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        public JdbcParamsBuilder setJdbcDriver(String jdbcDriver) {
            this.jdbcDriver = jdbcDriver;
            return this;
        }

        public JdbcParamsBuilder setPartitioningColumn(String partitioningColumn) {
            this.partitioningColumn = partitioningColumn;
            return this;
        }

        public JdbcParamsBuilder setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        public JdbcParamsBuilder setLocalityInfo(List<String> localityInfo) {
            this.localityInfo = localityInfo;
            return this;
        }

        public JdbcParams build() {
            return new JdbcParams(this);
        }
    }
}

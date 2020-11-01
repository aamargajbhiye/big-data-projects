package com.bugdbug.customsource.jdbc.utils;

import com.bugdbug.customsource.jdbc.Constants;
import com.bugdbug.customsource.jdbc.JdbcParams;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
import java.util.*;

public class JdbcUtil {
    public static Connection getConnection(JdbcParams jdbcParams)
            throws ClassNotFoundException, SQLException {
        Class.forName(jdbcParams.getJdbcDriver());
        Connection connection = DriverManager.getConnection(jdbcParams.getJdbcUrl(),
                jdbcParams.getUserName(), jdbcParams.getPassword());

        return connection;
    }

    public static ResultSetMetaData resultSetMetaData(JdbcParams jdbcParams) throws SQLException, ClassNotFoundException {
        String query = "Select * from " + jdbcParams.getTableName() + " where 1=0";
        Connection connection = getConnection(jdbcParams);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        return resultSet.getMetaData();
    }


    public static String getSqlFormattedColumnName(String name) {
        /*return "\"" + name.replaceAll("[ \t]", "_") + "\"";*/
        return "\"" + name + "\"";
    }

    public static List<Integer> getPartitionsColumnValues(JdbcParams jdbcParams)
            throws SQLException, ClassNotFoundException {
        Connection connection = JdbcUtil.getConnection(jdbcParams);
        String sql = "SELECT " + jdbcParams.getPartitioningColumn() + " FROM " + jdbcParams.getTableName();
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet rs = statement.executeQuery();
        List<Integer> columnValues = new ArrayList<>();
        while (rs.next()) {
            columnValues.add(rs.getInt(1));
        }
        return columnValues;
    }

    public static ResultSet readPartitionData(JdbcParams jdbcParams, Integer[] values)
            throws SQLException, ClassNotFoundException {
        Connection connection = JdbcUtil.getConnection(jdbcParams);
        String placeHolders = String.join(",", Collections.nCopies(values.length, "?"));
        String sql = "SELECT * from "
                + jdbcParams.getTableName()
                + " where " + jdbcParams.getPartitioningColumn()
                + " in (" + placeHolders + ")";
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            int i = 1;
            for (Integer param : values) {
                ps.setInt(i++, param);
            }
            return ps.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void createTable(StructType schema, JdbcParams jdbcParams) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = JdbcUtil.getConnection(jdbcParams);
            System.out.println("Creating table in given database..." + jdbcParams.getTableName());
            stmt = conn.createStatement();
            String query = generateQuery(schema, jdbcParams);
            stmt.executeUpdate(query);
            System.out.println("Created table successfully");
            // STEP 4: Clean-up environment
            stmt.close();
            conn.close();
        } catch (Exception se) {
            se.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException ignored) {
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    private static String generateQuery(StructType schema, JdbcParams jdbcParams) {
        StringBuilder queryBuilder = new StringBuilder("CREATE TABLE  ")
                .append(jdbcParams.getTableName()).append(" (");
        StructField[] fields = schema.fields();
        for (int i = 0; i < fields.length; i++) {
            StructField structField = fields[i];
            String name = JdbcUtil.getSqlFormattedColumnName(structField.name());
            String sqlDataType = SchemaUtil.getSqlDataType(structField.dataType());
            queryBuilder = queryBuilder.append(name).append(" ").append(sqlDataType);
            if (i < fields.length - 1)
                queryBuilder = queryBuilder.append(", ");
        }
        queryBuilder = queryBuilder.append(" )");
        String query = queryBuilder.toString();
        return query;
    }

    public static void dropTable(JdbcParams jdbcParams) {
        try {
            String dropTableQuery = "drop TABLE " + jdbcParams.getTableName();
            System.out.println("Dropping table: " + jdbcParams.getTableName());
            executeQuery(dropTableQuery, jdbcParams);
            System.out.println("Dropped table successful");
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }

    }

    public static void executeQuery(String query, JdbcParams jdbcParams) throws SQLException, ClassNotFoundException {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = JdbcUtil.getConnection(jdbcParams);
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            // STEP 4: Clean-up environment
            stmt.close();
            conn.close();
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException ignored) {
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    public static String createInsertQuery(StructType schema, JdbcParams jdbcParams) {
        String placeHolders = String.join(",", Collections.nCopies(schema.fieldNames().length, "?"));
        String sql = "insert into "
                + jdbcParams.getTableName()
                + " values ( " + placeHolders + " )";
        return sql;
    }

    public static void renameTable(String existingName, String newName, JdbcParams jdbcParams) {
        String renameQuery = "ALTER TABLE " + existingName + " RENAME TO " + newName;
        try {
            System.out.println(String.format("Renaming table: %s to %s ", existingName, newName));
            executeQuery(renameQuery, jdbcParams);
            System.out.println("Rename successful");
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }
    }
}
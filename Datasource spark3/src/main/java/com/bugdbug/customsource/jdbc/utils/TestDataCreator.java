package com.bugdbug.customsource.jdbc.utils;

import com.bugdbug.customsource.jdbc.JdbcParams;
import com.bugdbug.customsource.jdbc.utils.JdbcUtil;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestDataCreator {

    public static void createTable(JdbcParams jdbcParams) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = JdbcUtil.getConnection(jdbcParams);
            System.out.println("Creating table in given database..." + jdbcParams.getTableName());
            stmt = conn.createStatement();
            String sql = "CREATE TABLE  " + jdbcParams.getTableName() +
                    " (id INTEGER not NULL, " +
                    " first VARCHAR(255), " +
                    " last VARCHAR(255), " +
                    " age INTEGER, " +
                    " PRIMARY KEY ( id ))";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database..." + jdbcParams.getTableName());
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

    public static void createTestData(JdbcParams jdbcParams)
            throws SQLException, ClassNotFoundException {
        createTable(jdbcParams);
        insertTestData(jdbcParams);
    }


    private static void insertTestData(JdbcParams jdbcParams) throws SQLException, ClassNotFoundException {
        Connection connection = JdbcUtil.getConnection(jdbcParams);
        String sql = "INSERT INTO " + jdbcParams.getTableName() + " VALUES (?, ?, ?, ?)";
        PreparedStatement stmt = connection.prepareStatement(sql);

        Object[][] rows = {
                {100, "Zara", "Ali", 18},
                {101, "Mahnaz", "Fatma", 25},
                {102, "Zaid", "Khan", 30},
                {103, "Sumit", "Mittal", 28},
                {104, "Amar", "Gajbhiye", 30},
                {105, "Anthony", "G", 28},
                {106, "Jon", "Doe", 28},
                {107, "Shiv", "k", 25},
                {108, "Sunil", "L", 35}
        };


        for (Object[] row : rows) {
            try {
                stmt.setInt(1, (Integer) row[0]);
                stmt.setString(2, (String) row[1]);
                stmt.setString(3, (String) row[2]);
                stmt.setInt(4, (Integer) row[3]);
                stmt.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        System.out.println("Inserted records into the table..." + jdbcParams.getTableName());
        // STEP 4: Clean-up environment
        stmt.close();
        connection.close();
    }
}

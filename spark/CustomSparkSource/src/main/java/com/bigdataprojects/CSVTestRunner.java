package com.bigdataprojects;

import com.bigdataprojects.runnner.CustomDataSourceRunner;
import com.bigdataprojects.runnner.DataframeCreationRunner;
import com.bigdataprojects.runnner.SparkCSVSourceRunner;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class CSVTestRunner {
    public static void main(String args[]) {

        /**
         * Sales records csv can be downloaded from "http://eforexcel.com/wp/wp-content/uploads/2017/07/1000000%20Sales%20Records.7z"
         */
        String filePath = "<path_to_Sales Records.csv>";
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        while (true) {
            try {
                readInput(sparkSession, filePath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void readInput(SparkSession sparkSession, String filePath) {
        System.out.println("Select any of the following inputs \n1.Custom data source \n2.Spark CSV \n3.Create dataframe through dataload \n4.Exit");
        Scanner scanner = new Scanner(System.in);
        String input = scanner.next();

        Runnable runnable = selectTestRunner(input, sparkSession, filePath);
        runnable.run();
    }

    private static Runnable selectTestRunner(String input, SparkSession sparkSession, String filePath) {

        switch (input) {
            case "1":
                return new CustomDataSourceRunner(sparkSession, filePath);

            case "2":
                return new SparkCSVSourceRunner(sparkSession, filePath);

            case "3":
                return new DataframeCreationRunner(sparkSession, filePath);

            case "4":
                System.exit(0);
            default:
                throw new RuntimeException("Invalid input");
        }
    }
}



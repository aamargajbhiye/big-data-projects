package com.bigdataprojects.runnner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.concurrent.TimeUnit;

public class SparkCSVSourceRunner implements Runnable {

    private final SparkSession sparkSession;
    private final String filePath;

    public SparkCSVSourceRunner(SparkSession sparkSession, String filePath) {

        this.sparkSession = sparkSession;
        this.filePath = filePath;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        Dataset<Row> dataset = sparkSession.read().option("header", true).option("inferschema", true).csv(filePath);
       /* Dataset<Row> rowDataset = dataset.groupBy(dataset.col("Country"))
                .agg(
                        functions.sum("Units Sold").as("Total_Units_Sold"),
                        functions.sum("Total Revenue").as("Total_Revenue_Per_Country"),
                        functions.sum("Total Profit").as("Total_Profit_Per_Country")
                ).orderBy(functions.col("Total_Profit_Per_Country").desc());

        //rowDataset.explain();
        rowDataset.show();*/

        Dataset<Row> rowDataset = dataset.groupBy(dataset.col("Region"))
                .agg(
                        functions.sum("Total Revenue").as("Total_Revenue_Per_Country"),
                        functions.sum("Total Profit").as("Total_Profit_Per_Country")
                ).orderBy(functions.col("Total_Profit_Per_Country").desc());

        //rowDataset.explain();
        rowDataset.show();
        long endTime = System.currentTimeMillis();

        System.out.println("Time taken to perform the action is:-" + TimeUnit.MILLISECONDS.toSeconds(endTime - startTime));
    }
}

package com.bigdataprojects.customsource.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;

/**
 *
 */
public class CSVStreamingSourceRunner {

    public static void main(String[] args) throws IOException {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();

        writeSourceAsStream(sparkSession);
        //streamGroupBySaveResultAsFile(sparkSession);

    }


    private static void writeSourceAsStream(SparkSession sparkSession) {

        Dataset<Row> dataset = sparkSession.readStream().
                format("com.bigdataprojects.customsource.streaming.CSVStreamingSource")
                .option("filepath", "/home/amar/Downloads/1000000 Sales Records.csv")
                .load();

        StreamingQuery consoleQuery = dataset.writeStream().format("console").outputMode(OutputMode.Append()).start();

        try {
            consoleQuery.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void streamGroupBySaveResultAsFile(SparkSession sparkSession) {

        Dataset<Row> dataset = sparkSession.readStream().
                format("com.bigdataprojects.customstreamingsource.CSVStreamingSource")
                .option("filepath", "/home/amar/Downloads/1000000 Sales Records.csv")
                .load();

        Dataset<Row> groupedByDataset = dataset.groupBy(dataset.col("Region"))
                .agg(
                        functions.sum("Total Revenue").as("Total_Revenue_Per_Country"),
                        functions.sum("Total Profit").as("Total_Profit_Per_Country")
                );

        StreamingQuery streamingQuery = groupedByDataset.writeStream()
                .outputMode(OutputMode.Update())
                .format("csv")
                .option("path", "/home/amar/Downloads/path").start();

        try {
            streamingQuery.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }
}

package com.bigdataprojects.runnner;

import com.bigdataprojects.customsource.ValueConverter;
import com.opencsv.CSVReader;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DataframeCreationRunner implements Runnable{

    private final SparkSession sparkSession;
    private final String filePath;

    public DataframeCreationRunner(SparkSession sparkSession, String filePath){

        this.sparkSession = sparkSession;
        this.filePath = filePath;
    }
    @Override
    public void run() {
        StructField[] structFields = new StructField[]{
                new StructField("Region", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Country", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Item Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Sales Channel", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order Priority", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order ID", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Ship Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Units Sold", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Unit Price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Unit Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Revenue", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Profit", DataTypes.DoubleType, true, Metadata.empty())
        };

        StructType structType = new StructType(structFields);

        ValueConverter valueConverters[] = new ValueConverter[]{
                new ValueConverter.StringConverter(),
                new ValueConverter.StringConverter(),
                new ValueConverter.StringConverter(),
                new ValueConverter.StringConverter(),
                new ValueConverter.StringConverter(),
                new ValueConverter.StringConverter(),
                new ValueConverter.IntegerConverter(),
                new ValueConverter.StringConverter(),
                new ValueConverter.IntegerConverter(),
                new ValueConverter.DoubleConverter(),
                new ValueConverter.DoubleConverter(),
                new ValueConverter.DoubleConverter(),
                new ValueConverter.DoubleConverter(),
                new ValueConverter.DoubleConverter()
        };


        long startTime = System.currentTimeMillis();

        try (CSVReader csvReader = new CSVReader(new FileReader(filePath))) {
            Iterator<String[]> iterator = csvReader.iterator();
            Object[] convertedValues;
            iterator.next();//skipping header
            List<Row> rows = new ArrayList<>();
            while (iterator.hasNext()) {
                String[] row = iterator.next();
                convertedValues = new Object[row.length];
                for (int i = 0; i < row.length; i++)
                    convertedValues[i] = valueConverters[i].convertValue(row[i]);
                rows.add(RowFactory.create(convertedValues));
            }

            Dataset<Row> dataset = sparkSession.createDataFrame(rows, structType);
            Dataset<Row> rowDataset = dataset.groupBy(dataset.col("Country"))
                    .agg(
                            functions.sum("Units Sold").as("Total_Units_Sold"),
                            functions.sum("Total Revenue").as("Total_Revenue_Per_Country"),
                            functions.sum("Total Profit").as("Total_Profit_Per_Country")
                    ).orderBy(functions.col("Total_Profit_Per_Country").desc());

            //rowDataset.explain();
            rowDataset.show();
            long endTime = System.currentTimeMillis();

            System.out.println("Time taken to perform the action is:-" + TimeUnit.MILLISECONDS.toSeconds(endTime - startTime));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

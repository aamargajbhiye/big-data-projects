package com.bigdataprojects.customsource.streaming.reader;

import com.google.gson.Gson;
import com.opencsv.CSVReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 *
 */
public class CSVMicroBatchReader implements MicroBatchReader {

    private final String filePath;
    private int lastCommitted;
    private ReentrantLock reentrantLock;
    private int start;
    private int end;
    private int current;
    private List<Object[]> rows;
    private boolean keepRunning = false;
    private final CsvReader csvReader;
    private StructType schema;
    private List<Function> converters;

    public CSVMicroBatchReader(String filePath) {

        this.filePath = filePath;
        this.start = 0;
        this.end = 0;
        this.current = 0;
        this.rows = new ArrayList<>();
        createSchema();
        populateConverters();
        this.csvReader = new CsvReader(converters);
        new Thread(csvReader).start();
        this.reentrantLock = new ReentrantLock();
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
        this.start = ((CsvOffset) start.orElse(new CsvOffset(0))).getOffset();
        this.end = ((CsvOffset) end.orElse(new CsvOffset(0))).getOffset();
    }

    @Override
    public Offset getStartOffset() {
        return new CsvOffset(start);
    }

    @Override
    public Offset getEndOffset() {
        CsvOffset offset = new CsvOffset(current);
        return offset;
    }

    @Override
    public Offset deserializeOffset(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, CsvOffset.class);
    }

    @Override
    public void commit(Offset end) {

        int offset = ((CsvOffset) end).getOffset();
        int actualOffset = offset - lastCommitted;
        try {
            reentrantLock.lock();
            rows = new ArrayList<>(rows.subList(actualOffset, rows.size()));
        } finally {
            reentrantLock.unlock();
        }

        this.lastCommitted = offset;
    }

    @Override
    public void stop() {
        keepRunning = false;
        csvReader.close();
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        List<Object[]> rowChunk;
        try {
            int actualStart = start - lastCommitted;
            int actualEnd = end - lastCommitted;
            reentrantLock.lock();
            rowChunk = new ArrayList<>(rows.subList(actualStart, actualEnd));
        } finally {
            reentrantLock.unlock();
        }
        return Collections.singletonList(new CsvMicroBatchPartition(rowChunk));
    }

    private void createSchema() {
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
        this.schema = new StructType(structFields);
    }

    private void populateConverters() {
        StructField[] fields = this.schema.fields();
        converters = new ArrayList<>(fields.length);
        for (StructField structField : fields) {

            if (structField.dataType() == DataTypes.StringType)
                converters.add(stringConverter);
            else if (structField.dataType() == DataTypes.IntegerType)
                converters.add(intConverter);
            else if (structField.dataType() == DataTypes.DoubleType)
                converters.add(doubleConverter);
        }
    }

    /**
     *
     */
    class CsvReader implements Runnable, Serializable {
        private FileReader filereader;
        private Iterator<String[]> iterator;
        private List<Function> converters;

        CsvReader(List<Function> converters) {

            try {
                this.converters = converters;
                this.filereader = new FileReader(filePath);
                this.iterator = new CSVReader(filereader).iterator();
                iterator.next();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            keepRunning = true;
            while (iterator.hasNext() && keepRunning) {
                try {
                    String[] row = iterator.next();
                    Object[] convertedValues = new Object[row.length];
                    for (int i = 0; i < row.length; i++) {
                        convertedValues[i] = converters.get(i).apply(row[i]);
                    }
                    reentrantLock.lock();
                    rows.add(convertedValues);
                    current++;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    reentrantLock.unlock();
                }
            }
        }

        private void close() {
            try {
                filereader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private Function<String, UTF8String> stringConverter = (val) -> UTF8String.fromString(val);

    private Function<String, Integer> intConverter = (val) -> Integer.parseInt(val);

    private Function<String, Double> doubleConverter = (val) -> Double.parseDouble(val);
}

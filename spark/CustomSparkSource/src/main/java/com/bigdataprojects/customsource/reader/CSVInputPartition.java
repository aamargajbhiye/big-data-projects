package com.bigdataprojects.customsource.reader;

import com.opencsv.CSVReader;
import com.bigdataprojects.customsource.ValueConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import scala.collection.JavaConverters;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This class handles the per partition data read
 */
public class CSVInputPartition implements InputPartition<InternalRow> {

    private String filePath;

    public CSVInputPartition(String filePath) {

        this.filePath = filePath;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {

        return new CSVInputPartitionReader(filePath);
    }
}

class CSVInputPartitionReader implements InputPartitionReader<InternalRow> {

    private Iterator<String[]> iterator;
    private CSVReader csvReader;
    private ValueConverter[] valueConverters;

    public CSVInputPartitionReader(String filePath) {
        FileReader filereader = null;
        try {
            filereader = new FileReader(filePath);
            csvReader = new CSVReader(filereader);
            iterator = csvReader.iterator();
            iterator.next();
            initializeValueConverters();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    @Override
    public boolean next() throws IOException {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        Object[] values = iterator.next();
        Object[] convertedValues = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            convertedValues[i] = valueConverters[i].convertValue((String) values[i]);
        }
        return InternalRow.apply(JavaConverters.asScalaIteratorConverter(Arrays.asList(convertedValues).iterator()).asScala().toSeq());
    }

    @Override
    public void close() throws IOException {
        csvReader.close();
    }

    private void initializeValueConverters() {
        valueConverters = new ValueConverter[]{
                new ValueConverter.UTF8StringConverter(),
                new ValueConverter.UTF8StringConverter(),
                new ValueConverter.UTF8StringConverter(),
                new ValueConverter.UTF8StringConverter(),
                new ValueConverter.UTF8StringConverter(),
                new ValueConverter.UTF8StringConverter(),
                new ValueConverter.IntegerConverter(),
                new ValueConverter.UTF8StringConverter(),
                new ValueConverter.IntegerConverter(),
                new ValueConverter.DoubleConverter(),
                new ValueConverter.DoubleConverter(),
                new ValueConverter.DoubleConverter(),
                new ValueConverter.DoubleConverter(),
                new ValueConverter.DoubleConverter()
        };
    }

};

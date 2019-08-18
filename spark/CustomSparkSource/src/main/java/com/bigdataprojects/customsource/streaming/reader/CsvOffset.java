package com.bigdataprojects.customsource.streaming.reader;

import com.google.gson.Gson;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

public class CsvOffset extends Offset {

    private int offset;

    public CsvOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public String json() {

        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getOffset() {
        return offset;
    }
}

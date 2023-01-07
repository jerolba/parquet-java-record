package com.jerolba.carpet;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.parquet.hadoop.ParquetWriter;

import com.jerolba.parquet.record.OutputStreamOutputFile;
import com.jerolba.parquet.record.ParquetRecordReader;

public class ParquetWriterTest<T> {

    private final String path;
    private final Class<T> type;
    private AnnotatedLevels level = AnnotatedLevels.THREE;

    ParquetWriterTest(String path, Class<T> type) {
        this.path = path;
        this.type = type;
        new File(path).delete();
    }

    public ParquetWriterTest<T> withLevel(AnnotatedLevels level) {
        this.level = level;
        return this;
    }

    public void write(T... values) throws IOException {
        OutputStreamOutputFile output = new OutputStreamOutputFile(new FileOutputStream(path));
        try (ParquetWriter<T> writer = CarpetWriter.builder(output, type)
                .levelStructure(level)
                .build()) {
            for (var v : values) {
                writer.write(v);
            }
        }
    }

    public Iterator<T> getReadIterator() throws IOException {
        var reader = new ParquetRecordReader<>(path, type);
        return reader.iterator();
    }
}
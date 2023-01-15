package com.jerolba.carpet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import com.jerolba.parquet.record.FileSystemInputFile;
import com.jerolba.parquet.record.OutputStreamOutputFile;
import com.jerolba.parquet.record.ParquetRecordReader;

public class ParquetWriterTest<T> {

    private final Class<T> type;
    private String path;
    private AnnotatedLevels level = AnnotatedLevels.THREE;

    ParquetWriterTest(Class<T> type) {
        String fileName = type.getName() + ".parquet";
        this.path = "/tmp/" + fileName;
        try {
            java.nio.file.Path targetPath = Files.createTempFile("parquet", fileName);
            this.path = targetPath.toFile().getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.type = type;
        new File(path).delete();
    }

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
        write(List.of(values));
    }

    public void writeAndAssertReadIsEquals(T... values) throws IOException {
        List<T> asList = List.of(values);
        write(asList);
        ParquetReader<T> reader = getCarpetReader();
        int i = 0;
        T read = null;
        while ((read = reader.read()) != null) {
            assertEquals(asList.get(i), read);
            i++;
        }
    }

    public void write(Collection<T> values) throws IOException {
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

    public ParquetReader<GenericRecord> getGenericRecordReader() throws IOException {
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                new FileSystemInputFile(new File(path)))
                .withDataModel(GenericData.get())
                .build();
        return reader;
    }

    public ParquetReader<T> getCarpetReader() throws IOException {
        Path filePath = new Path(path);
        return new CarpetReader<T>().read(filePath, type);
    }
}
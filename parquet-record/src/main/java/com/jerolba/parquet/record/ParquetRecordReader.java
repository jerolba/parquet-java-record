package com.jerolba.parquet.record;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import com.jerolba.avro.record.AvroRecord2JavaRecord;

public class ParquetRecordReader<T> {

    private final String path;
    private final Class<T> recordClass;

    public ParquetRecordReader(String path, Class<T> recordClass) throws IOException {
        this.path = path;
        this.recordClass = recordClass;
    }

    public Iterator<T> iterator() throws IOException {
        InputFile file = HadoopInputFile.fromPath(new Path(path), new Configuration());
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
        return new RecordIterator<T>(recordClass, reader);
    }

    public Stream<T> stream() throws IOException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    private class RecordIterator<T> implements Iterator<T>, Closeable {

        private final ParquetReader<GenericRecord> reader;
        private final AvroRecord2JavaRecord<T> mapper;
        private GenericRecord nextRecord;

        public RecordIterator(Class<T> recordClass, ParquetReader<GenericRecord> reader) throws IOException {
            this.reader = reader;
            nextRecord = reader.read();
            if (nextRecord != null) {
                Schema schema = nextRecord.getSchema();
                mapper = new AvroRecord2JavaRecord<>(recordClass, schema);
            } else {
                mapper = null;
            }
        }

        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T res = mapper.mapMainRecord(nextRecord);
            try {
                this.nextRecord = reader.read();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return res;
        }

        @Override
        public void close() throws IOException {
            reader.close();
            nextRecord = null;
        }

    }

}
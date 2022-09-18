/**
 * Copyright 2022 Jerónimo López Bezanilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jerolba.parquet.record;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import com.jerolba.avro.record.AvroRecord2JavaRecord;

public class ParquetRecordReader<T> {

    private final InputFile inputFile;
    private final Class<T> recordClass;

    public ParquetRecordReader(String path, Class<T> recordClass) throws IOException {
        this(new FileSystemInputFile(new File(path)), recordClass);
    }

    public ParquetRecordReader(InputFile inputFile, Class<T> recordClass) throws IOException {
        this.inputFile = inputFile;
        this.recordClass = recordClass;
    }

    public Iterator<T> iterator() throws IOException {
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build();
        return new RecordIterator<>(recordClass, reader);
    }

    public Stream<T> stream() throws IOException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    private class RecordIterator<T> implements Iterator<T>, Closeable {

        private final ParquetReader<GenericRecord> reader;
        private final AvroRecord2JavaRecord<T> mapper;
        private GenericRecord nextRecord;

        RecordIterator(Class<T> recordClass, ParquetReader<GenericRecord> reader) throws IOException {
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
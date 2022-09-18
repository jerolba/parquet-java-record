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
package com.jerolba.avro.record;

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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvroRecordReader<T> {

    private final String path;
    private final Class<T> recordClass;

    public AvroRecordReader(String path, Class<T> recordClass) throws IOException {
        this.path = path;
        this.recordClass = recordClass;
    }

    public Iterator<T> iterator() throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(path), datumReader);
        return new RecordIterator<>(recordClass, dataFileReader);
    }

    public Stream<T> stream() throws IOException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    private class RecordIterator<T> implements Iterator<T>, Closeable {

        private final DataFileReader<GenericRecord> reader;
        private final AvroRecord2JavaRecord<T> mapper;

        RecordIterator(Class<T> recordClass, DataFileReader<GenericRecord> reader) throws IOException {
            this.reader = reader;
            Schema schema = reader.getSchema();
            mapper = new AvroRecord2JavaRecord<>(recordClass, schema);
        }

        @Override
        public boolean hasNext() {
            return reader.hasNext();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return mapper.mapMainRecord(reader.next());
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

    }

}
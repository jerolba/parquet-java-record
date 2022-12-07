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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    /**
     * Constructs a new AvroRecordReader to read records of the specified type from
     * the specified Avro file.
     *
     * @param path        the path to the Avro file
     * @param recordClass the class of the records to be read
     * @throws IOException if an I/O error occurs while opening the file
     */
    public AvroRecordReader(String path, Class<T> recordClass) throws IOException {
        this.path = path;
        this.recordClass = recordClass;
    }

    /**
     * Returns an iterator over the records in this AvroRecordReader in the order in
     * which they are read from the Avro file.
     *
     * @return an iterator over the records in this AvroRecordReader
     * @throws IOException if an I/O error occurs while reading the file
     */
    public Iterator<T> iterator() throws IOException {
        return buildIterator();
    }

    /**
     * Returns a sequential stream with this AvroRecordReader as its source.
     *
     * The returned stream encapsulates a Reader. If timely disposal of file system
     * resources is required, the try-with-resources construct should be used to
     * ensure that the stream's close method is invoked after the stream operations
     * are completed.
     *
     * @return a sequential stream with this AvroRecordReader as its source
     * @throws IOException if an I/O error occurs while reading the file
     */
    public Stream<T> stream() throws IOException {
        RecordIterator<T> iterator = buildIterator();
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
        return StreamSupport.stream(spliterator, false)
                .onClose(() -> iterator.uncheckedCloseReader());
    }

    /**
     * Returns a list with all the records read by this AvroRecordReader.
     *
     * @return a list with all the records read by this AvroRecordReader
     * @throws IOException if an I/O error occurs while reading the file
     */
    public List<T> toList() throws IOException {
        List<T> result = new ArrayList<>();
        try (var iterator = buildIterator()) {
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
            return result;
        }
    }

    private RecordIterator<T> buildIterator() throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(path), datumReader);
        return new RecordIterator<>(recordClass, dataFileReader);
    }

    private class RecordIterator<R> implements Iterator<R>, AutoCloseable {

        private final DataFileReader<GenericRecord> reader;
        private final AvroRecord2JavaRecord<R> mapper;

        /**
         * Constructs a new RecordIterator to iterate over the records in the specified
         * DataFileReader.
         *
         * @param recordClass the class of the records to be read
         * @param reader      the DataFileReader to read the records from
         * @throws IOException if an I/O error occurs while reading the file
         */
        RecordIterator(Class<R> recordClass, DataFileReader<GenericRecord> reader) throws IOException {
            this.reader = reader;
            Schema schema = reader.getSchema();
            mapper = new AvroRecord2JavaRecord<>(recordClass, schema);
        }

        /**
         * Returns {@code true} if the iteration has more records.
         *
         * @return {@code true} if the iteration has more records
         */
        @Override
        public boolean hasNext() {
            boolean hasNext = reader.hasNext();
            if (!hasNext) {
                uncheckedCloseReader();
            }
            return hasNext;
        }

        /**
         * Returns the next record in the iteration.
         *
         * @return the next record in the iteration
         * @throws NoSuchElementException if the iteration has no more records
         */
        @Override
        public R next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return mapper.mapMainRecord(reader.next());
        }

        /**
         * Closes the underlying reader and releases any resources associated with it.
         *
         * @throws IOException if an I/O error occurs while closing the reader
         */
        @Override
        public void close() throws IOException {
            reader.close();
        }

        /**
         * Closes the underlying reader and releases any resources associated with it,
         * suppressing any checked exceptions.
         */
        private void uncheckedCloseReader() {
            try {
                close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

    }

}
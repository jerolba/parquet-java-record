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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import com.jerolba.avro.record.AvroRecord2JavaRecord;

/**
 *
 * This class reads Parquet files and provides access to their data through an
 * {@link Iterator} or {@link Stream}.
 *
 * The records in the Parquet file are mapped to instances of the specified
 * class {@code T} using Avro.
 *
 * @param <T> the type of the records in the Parquet file
 *
 */
public class ParquetRecordReader<T> {

    private final InputFile inputFile;
    private final Class<T> recordClass;

    /**
     *
     * Creates a new {@code ParquetRecordReader} instance from the specified path
     * and record class.
     *
     * @param path        the path to the Parquet file
     * @param recordClass the class of the records in the Parquet file
     * @throws IOException if an I/O error occurs
     */
    public ParquetRecordReader(String path, Class<T> recordClass) throws IOException {
        this(new FileSystemInputFile(new File(path)), recordClass);
    }

    /**
     *
     * Creates a new {@code ParquetRecordReader} instance from the specified input
     * file and record class.
     *
     * @param inputFile   the input file containing the Parquet data
     * @param recordClass the class of the records in the Parquet file
     * @throws IOException if an I/O error occurs
     */
    public ParquetRecordReader(InputFile inputFile, Class<T> recordClass) throws IOException {
        this.inputFile = inputFile;
        this.recordClass = recordClass;
    }

    /**
     *
     * Returns an {@link Iterator} that can be used to iterate over the records in
     * the Parquet file.
     *
     * @return an iterator for the records in the Parquet file
     * @throws IOException if an I/O error occurs
     */
    public Iterator<T> iterator() throws IOException {
        return buildIterator();
    }

    /**
     *
     * Returns a {@link Stream} that can be used to access the records in the
     * Parquet file.
     *
     * @return a stream for the records in the Parquet file
     * @throws IOException if an I/O error occurs
     */
    public Stream<T> stream() throws IOException {
        RecordIterator<T> iterator = buildIterator();
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
        return StreamSupport.stream(spliterator, false)
                .onClose(() -> iterator.uncheckedCloseReader());
    }

    /**
     *
     * Returns a {@link List} containing all records in the Parquet file.
     *
     * @return a list of all records in the Parquet file
     * @throws IOException if an I/O error occurs
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
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                .withDataModel(GenericData.get())
                .build();
        return new RecordIterator<>(recordClass, reader);
    }

    /**
     * This class provides an iterator for the records in a Parquet file.
     *
     * @param <R> the type of the records in the Parquet file
     */
    private class RecordIterator<R> implements Iterator<R>, Closeable {

        private final ParquetReader<GenericRecord> reader;
        private final AvroRecord2JavaRecord<R> mapper;
        private GenericRecord nextRecord;

        /**
         * Creates a new {@code RecordIterator} instance from the specified record class
         * and reader.
         *
         * @param recordClass the class of the records in the Parquet file
         * @param reader      the reader for the Parquet data
         * @throws IOException if an I/O error occurs
         */
        RecordIterator(Class<R> recordClass, ParquetReader<GenericRecord> reader) throws IOException {
            this.reader = reader;
            nextRecord = reader.read();
            if (nextRecord != null) {
                Schema schema = nextRecord.getSchema();
                mapper = new AvroRecord2JavaRecord<>(recordClass, schema);
            } else {
                mapper = null;
            }
        }

        /**
         * Returns {@code true} if the iteration has more records.
         *
         * @return {@code true} if the iteration has more records, {@code false}
         *         otherwise
         */
        @Override
        public boolean hasNext() {
            return nextRecord != null;
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
            R res = mapper.mapMainRecord(nextRecord);
            try {
                this.nextRecord = reader.read();
                if (nextRecord == null) {
                    uncheckedCloseReader();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return res;
        }

        /**
         * Closes the underlying reader.
         *
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void close() throws IOException {
            reader.close();
            nextRecord = null;
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
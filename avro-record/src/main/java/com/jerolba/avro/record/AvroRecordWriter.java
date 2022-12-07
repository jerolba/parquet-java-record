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
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

public class AvroRecordWriter<T> {

    private final Schema schema;
    private final JavaRecord2AvroRecord<T> mapper;

    /**
     * Constructs a new AvroRecordWriter to write records of the specified type to
     * an Avro file.
     *
     * @param recordClass the class of the records to be written
     * @throws IOException if an I/O error occurs while building the schema for the
     *                     records
     */
    public AvroRecordWriter(Class<T> recordClass) throws IOException {
        JavaRecord2Schema toSchema = new JavaRecord2Schema();
        schema = toSchema.build(recordClass);
        mapper = new JavaRecord2AvroRecord<>(recordClass, schema);
    }

    /**
     * Writes the specified collection of records to the Avro file with the
     * specified path.
     *
     * @param targetPath the path of the Avro file to write the records to
     * @param collection the collection of records to write
     * @throws IOException if an I/O error occurs while writing to the file
     */
    public void write(String targetPath, Collection<T> collection) throws IOException {
        write(targetPath, collection.stream());
    }

    /**
     * Writes the specified collection of records to the specified OutputStream.
     *
     * @param outputStream the OutputStream to write the records to
     * @param collection   the collection of records to write
     * @throws IOException if an I/O error occurs while writing to the OutputStream
     */
    public void write(OutputStream outputStream, Collection<T> collection) throws IOException {
        write(outputStream, collection.stream());
    }

    /**
     * Writes the records in the specified stream to the Avro file with the
     * specified path.
     *
     * @param targetPath the path of the Avro file to write the records to
     * @param stream     the stream of records to write
     * @throws IOException if an I/O error occurs while writing to the file
     */
    public void write(String targetPath, Stream<T> stream) throws IOException {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            writeAll(dataFileWriter.create(schema, new File(targetPath)), stream);
        }
    }

    /**
     * Writes the records in the specified stream to the specified OutputStream.
     *
     * @param outputStream the OutputStream to write the records to
     * @param stream       the stream of records to write
     * @throws IOException if an I/O error occurs while writing to the OutputStream
     */
    public void write(OutputStream outputStream, Stream<T> stream) throws IOException {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            writeAll(dataFileWriter.create(schema, outputStream), stream);
        }
    }

    private void writeAll(DataFileWriter<GenericRecord> writer, Stream<T> stream) throws IOException {
        Iterator<T> it = stream.iterator();
        while (it.hasNext()) {
            writer.append(mapper.mapRecord(it.next()));
        }
    }

}

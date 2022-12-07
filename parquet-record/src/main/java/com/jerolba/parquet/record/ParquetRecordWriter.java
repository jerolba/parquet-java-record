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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;

import com.jerolba.avro.record.JavaRecord2AvroRecord;
import com.jerolba.avro.record.JavaRecord2Schema;

/**
 *
 * Class for writing Parquet records from a collection or stream of Java
 * objects.
 *
 * @param <T> the type of the Java objects to be written as Parquet records
 *
 */
public class ParquetRecordWriter<T> {

    private final Schema schema;
    private final JavaRecord2AvroRecord<T> mapper;
    private final ParquetRecordWriterConfig<T> config;

    /**
     *
     * Constructs a ParquetRecordWriter with the specified configuration.
     *
     * @param config the configuration for the writer
     * @throws IOException if an error occurs while creating the writer
     */
    public ParquetRecordWriter(ParquetRecordWriterConfig<T> config) throws IOException {
        this.config = config;
        JavaRecord2Schema toSchema = new JavaRecord2Schema();
        schema = toSchema.build(config.getRecordClass());
        mapper = new JavaRecord2AvroRecord<>(config.getRecordClass(), schema);
    }

    /**
     *
     * Writes the specified collection of Java objects to a Parquet file.
     *
     * @param collection the collection of objects to write
     * @throws IOException if an error occurs while writing the records
     */
    public void write(Collection<T> collection) throws IOException {
        this.write(collection.stream());
    }

    /**
     * 
     * Writes the specified stream of Java objects to a Parquet file.
     * 
     * @param stream the stream of objects to write
     * 
     * @throws IOException if an error occurs while writing the records
     */
    public void write(Stream<T> stream) throws IOException {
        try (ParquetWriter<GenericRecord> writer = config
                .getWriterBuilder()
                .withSchema(schema)
                .build()) {

            Iterator<T> it = stream.iterator();
            while (it.hasNext()) {
                writer.write(mapper.mapRecord(it.next()));
            }
        }
    }

}
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

public class ParquetRecordWriter<T> {

    private final Schema schema;
    private final JavaRecord2AvroRecord<T> mapper;
    private final ParquetRecordWriterConfig<T> config;

    public ParquetRecordWriter(ParquetRecordWriterConfig<T> config) throws IOException {
        this.config = config;
        JavaRecord2Schema toSchema = new JavaRecord2Schema();
        schema = toSchema.build(config.getRecordClass());
        mapper = new JavaRecord2AvroRecord<>(config.getRecordClass(), schema);
    }

    public void write(Collection<T> collection) throws IOException {
        this.write(collection.stream());
    }

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
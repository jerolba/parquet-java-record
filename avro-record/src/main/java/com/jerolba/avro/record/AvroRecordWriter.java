package com.jerolba.avro.record;

import java.io.File;
import java.io.IOException;
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

    public AvroRecordWriter(Class<T> recordClass) throws IOException {
        JavaRecord2Schema toSchema = new JavaRecord2Schema();
        schema = toSchema.build(recordClass);
        mapper = new JavaRecord2AvroRecord<>(recordClass, schema);
    }

    public void write(String targetPath, Collection<T> collection) throws IOException {
        this.write(targetPath, collection.stream());
    }

    public void write(String targetPath, Stream<T> stream) throws IOException {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter)) {
            DataFileWriter<GenericRecord> writer = dataFileWriter.create(schema, new File(targetPath));
            Iterator<T> it = stream.iterator();
            while (it.hasNext()) {
                writer.append(mapper.mapRecord(it.next()));
            }
        }
    }

}

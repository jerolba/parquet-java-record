package com.jerolba.parquet.record;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import com.jerolba.avro.record.JavaRecord2AvroRecord;
import com.jerolba.avro.record.JavaRecord2Schema;

public class ParquetRecordWriter<T> {

    private final Schema schema;
    private final JavaRecord2AvroRecord<T> mapper;

    public ParquetRecordWriter(Class<T> recordClass) throws IOException {
        JavaRecord2Schema toSchema = new JavaRecord2Schema();
        schema = toSchema.build(recordClass);
        mapper = new JavaRecord2AvroRecord<>(recordClass, schema);
    }

    public void write(String targetPath, Collection<T> collection) throws IOException {
        this.write(targetPath, collection.stream());
    }

    public void write(String targetPath, Stream<T> stream) throws IOException {
        OutputFile output = getOutputFile(targetPath);
//        Path path = new Path(targetPath);
//        HadoopOutputFile hadoop = HadoopOutputFile.fromPath(path, new Configuration());
//        var output = new HadoopOutputWrapper(hadoop);
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(output)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withConf(new Configuration())
                .withValidation(true)
                .withDictionaryEncoding(true)
                .withWriteMode(Mode.OVERWRITE)
                .build()) {

            Iterator<T> it = stream.iterator();
            while (it.hasNext()) {
                writer.write(mapper.mapRecord(it.next()));
            }
        }
    }

    private OutputFile getOutputFile(String targetPath) throws FileNotFoundException {
        return new SimpleOutputStreamOutputFile(new FileOutputStream(targetPath));

    }

}
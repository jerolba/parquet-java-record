package com.jerolba.tarima;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class TarimaWriter<T> {

    public static <T> Builder<T> builder(OutputFile file, Class<T> recordClass) {
        return new Builder<>(file, recordClass);
    }

    public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {

        private final Class<T> recordClass;
        private Map<String, String> extraMetaData = new HashMap<>();

        private Builder(OutputFile file, Class<T> recordClass) {
            super(file);
            this.recordClass = recordClass;
        }

        public Builder<T> withExtraMetaData(Map<String, String> extraMetaData) {
            this.extraMetaData = extraMetaData;
            return this;
        }

        @Override
        protected Builder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return new TarimaWriterSupport<>(recordClass, extraMetaData);
        }
    }

    private static class TarimaWriterSupport<T> extends WriteSupport<T> {

        private final Class<T> recordClass;
        private final Map<String, String> extraMetaData;
        private TarimaRecordWriter<T> tarimaWriter;

        public TarimaWriterSupport(Class<T> recordClass, Map<String, String> extraMetaData) {
            this.recordClass = recordClass;
            this.extraMetaData = extraMetaData;
        }

        @Override
        public String getName() {
            return recordClass.getName();
        }

        @Override
        public WriteContext init(Configuration configuration) {
            JavaRecord2Schema javaRecord2Schema = new JavaRecord2Schema();
            MessageType schema = javaRecord2Schema.createSchema(recordClass);
            return new WriteContext(schema, this.extraMetaData);
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            try {
                tarimaWriter = new TarimaRecordWriter<>(recordConsumer, recordClass);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public void write(T record) {
            tarimaWriter.write(record);
        }
    }

}

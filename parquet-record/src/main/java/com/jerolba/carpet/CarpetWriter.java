package com.jerolba.carpet;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class CarpetWriter<T> {

    public static <T> Builder<T> builder(OutputFile file, Class<T> recordClass) {
        return new Builder<>(file, recordClass);
    }

    public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {

        private final Class<T> recordClass;
        private Map<String, String> extraMetaData = new HashMap<>();
        private AnnotatedLevels annotatedLevels = AnnotatedLevels.THREE;

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

        public Builder<T> levelStructure(AnnotatedLevels annotatedLevels) {
            this.annotatedLevels = annotatedLevels;
            return self();
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            CarpetConfiguration carpetCfg = new CarpetConfiguration(annotatedLevels);
            return new TarimaWriterSupport<>(recordClass, extraMetaData, carpetCfg);
        }

    }

    private static class TarimaWriterSupport<T> extends WriteSupport<T> {

        private final Class<T> recordClass;
        private final Map<String, String> extraMetaData;
        private final CarpetConfiguration carpetConfiguration;
        private CarpetMessageWriter<T> tarimaWriter;

        public TarimaWriterSupport(Class<T> recordClass, Map<String, String> extraMetaData,
                CarpetConfiguration carpetConfiguration) {
            this.recordClass = recordClass;
            this.extraMetaData = extraMetaData;
            this.carpetConfiguration = carpetConfiguration;
        }

        @Override
        public String getName() {
            return recordClass.getName();
        }

        @Override
        public WriteContext init(Configuration configuration) {
            JavaRecord2Schema javaRecord2Schema = new JavaRecord2Schema(carpetConfiguration);
            MessageType schema = javaRecord2Schema.createSchema(recordClass);
            return new WriteContext(schema, this.extraMetaData);
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            try {
                tarimaWriter = new CarpetMessageWriter<>(recordConsumer, recordClass, carpetConfiguration);
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

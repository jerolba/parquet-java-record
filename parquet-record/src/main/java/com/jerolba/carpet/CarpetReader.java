package com.jerolba.carpet;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import com.jerolba.carpet.impl.read.CarpetGroupConverter;
import com.jerolba.carpet.impl.read.SchemaFilter;
import com.jerolba.carpet.impl.read.SchemaValidation;

public class CarpetReader<T> {

    // TODO: inject values via config reader. Which default values?
    private boolean ignoreUnknown = false;
    private boolean strictNumericType = false;

    public CarpetReader<T> ignoreUnknown(boolean ignoreUnknown) {
        this.ignoreUnknown = ignoreUnknown;
        return this;
    }

    public CarpetReader<T> strictNumericType(boolean strictNumericType) {
        this.strictNumericType = strictNumericType;
        return this;
    }

    public ParquetReader<T> read(Path file, Class<T> readClass) throws IOException {
        CarpetReadConfiguration configuration = new CarpetReadConfiguration(ignoreUnknown, strictNumericType);
        CarpetReadSupport<T> readSupport = new CarpetReadSupport<>(readClass, configuration);
        return ParquetReader.builder(readSupport, file).build();
    }

    public static class CarpetReadSupport<T> extends ReadSupport<T> {

        private final Class<T> readClass;
        private final CarpetReadConfiguration carpetConfiguration;

        public CarpetReadSupport(Class<T> readClass, CarpetReadConfiguration carpetConfiguration) {
            this.readClass = readClass;
            this.carpetConfiguration = carpetConfiguration;
        }

        @Override
        public RecordMaterializer<T> prepareForRead(Configuration configuration,
                Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
            return new CarpetMaterializer<>(readClass, readContext.getRequestedSchema());
        }

        @Override
        public ReadContext init(Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema) {

            var validation = new SchemaValidation(carpetConfiguration.isIgnoreUnknown(),
                    carpetConfiguration.isStrictNumericType());
            SchemaFilter projectedSchema = new SchemaFilter(validation, fileSchema);
            MessageType projection = projectedSchema.project(readClass);
            Map<String, String> metadata = new LinkedHashMap<>();
            return new ReadContext(projection, metadata);
        }

    }

    static class CarpetMaterializer<T> extends RecordMaterializer<T> {

        private final CarpetGroupConverter root;
        private T value;

        public CarpetMaterializer(Class<T> readClass, MessageType requestedSchema) {
            this.root = new CarpetGroupConverter(requestedSchema, readClass, record -> this.value = (T) record);
        }

        @Override
        public T getCurrentRecord() {
            return value;
        }

        @Override
        public GroupConverter getRootConverter() {
            return root;
        }

    }

}

package com.jerolba.carpet;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.impl.read.CarpetGroupConverter;

public class CarpetReader<T> {

    public ParquetReader<T> read(Path file, Class<T> readClass) throws IOException {
        return ParquetReader.builder(new CarpetReadSupport<>(readClass), file).build();
    }

    public static class CarpetReadSupport<T> extends ReadSupport<T> {

        private final Class<T> readClass;

        public CarpetReadSupport(Class<T> readClass) {
            this.readClass = readClass;
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

            // TODO: match class schema with file schema to make the projection
            // List<Type> list = fileSchema.getFields().stream().filter(f ->
            // !f.getName().equals("category")).toList();
            List<Type> list = fileSchema.getFields();

            MessageType projection = new MessageType(fileSchema.getName(), list);
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

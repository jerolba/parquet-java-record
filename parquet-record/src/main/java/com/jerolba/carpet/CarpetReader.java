package com.jerolba.carpet;

import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromBinaryConverter;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromBooleanConverter;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromDoubleConverter;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromFloatConverter;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromInt32;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromInt64Converter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.impl.read.GroupFieldsMapper;

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
            List<Type> list = fileSchema.getFields().stream().filter(f -> !f.getName().equals("category")).toList();

            MessageType projection = new MessageType(fileSchema.getName(), list);
            Map<String, String> metadata = new LinkedHashMap<>();

            return new ReadContext(projection, metadata);
        }

    }

    static class CarpetMaterializer<T> extends RecordMaterializer<T> {

        private final CarpetConverter root;

        public CarpetMaterializer(Class<T> readClass, MessageType requestedSchema) {
            this.root = new CarpetConverter(readClass, requestedSchema);
        }

        @Override
        public T getCurrentRecord() {
            return (T) root.getCurrentRecord();
        }

        @Override
        public GroupConverter getRootConverter() {
            return root;
        }
    }

    static class CarpetConverter extends GroupConverter {

        private final GroupType requestedSchema;
        private final Class<?> groupClass;

        private final Converter[] converters;
        private final ConstructorParams constructor;

        public CarpetConverter(Class<?> groupClass, GroupType requestedSchema) {
            this.groupClass = groupClass;
            this.requestedSchema = requestedSchema;
            this.constructor = new ConstructorParams(groupClass);
            System.out.println(requestedSchema);

            GroupFieldsMapper mapper = new GroupFieldsMapper(groupClass);

            // Discover converters
            converters = new Converter[requestedSchema.getFields().size()];
            int cont = 0;
            for (var f : requestedSchema.getFields()) {
                String name = f.getName();
                Repetition repetition = f.getRepetition();
                int index = mapper.getIndex(name);
                var recordComponent = mapper.getRecordComponent(name);
                if (recordComponent == null) {
                    throw new RecordTypeConversionException(
                            groupClass.getName() + " doesn't have an attribute called " + name);
                }
                if (f.isPrimitive()) {
                    PrimitiveType asPrimitive = f.asPrimitiveType();
                    PrimitiveTypeName type = asPrimitive.getPrimitiveTypeName();
                    switch (type) {
                    case INT32:
                        converters[cont] = buildFromInt32(constructor, index, recordComponent);
                        break;
                    case INT64:
                        converters[cont] = buildFromInt64Converter(constructor, index, recordComponent);
                        break;
                    case FLOAT:
                        converters[cont] = buildFromFloatConverter(constructor, index, recordComponent);
                        break;
                    case DOUBLE:
                        converters[cont] = buildFromDoubleConverter(constructor, index, recordComponent);
                        break;
                    case BOOLEAN:
                        converters[cont] = buildFromBooleanConverter(constructor, index, recordComponent);
                        break;
                    case BINARY:
                        converters[cont] = buildFromBinaryConverter(constructor, index, recordComponent, f);
                        break;
                    case INT96, FIXED_LEN_BYTE_ARRAY:
                        throw new RecordTypeConversionException(type + " deserialization not supported");
                    }
                } else {
                    GroupType asGroupType = f.asGroupType();
                    LogicalTypeAnnotation logicalType = asGroupType.getLogicalTypeAnnotation();
                    if (logicalType == LogicalTypeAnnotation.listType()) {

                    } else if (logicalType == LogicalTypeAnnotation.mapType()) {

                    } else {
                        Class<?> childClass = recordComponent.getType();
                        CarpetGroupConverter converter = new CarpetGroupConverter(childClass, asGroupType,
                                constructor, index, recordComponent, f);
                        converters[cont] = converter;
                    }
                }
                cont++;
            }
        }

        public Object getCurrentRecord() {
            return constructor.create();
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }

        @Override
        public void start() {
            Arrays.fill(constructor.c, null);

        }

        @Override
        public void end() {
        }

    }

    static class CarpetGroupConverter extends GroupConverter {

        private final GroupType requestedSchema;
        private final Class<?> groupClass;

        private final Converter[] converters;
        private final ConstructorParams constructor;
        private final ConstructorParams parentConstructor;
        private final int parentIndex;

        public CarpetGroupConverter(Class<?> groupClass, GroupType requestedSchema, ConstructorParams parentConstructor,
                int parentIndex, RecordComponent parentRecordComponent, Type parentSchemaType) {
            this.groupClass = groupClass;
            this.requestedSchema = requestedSchema;
            this.parentConstructor = parentConstructor;
            this.parentIndex = parentIndex;
            this.constructor = new ConstructorParams(groupClass);
            System.out.println(requestedSchema);

            GroupFieldsMapper mapper = new GroupFieldsMapper(groupClass);

            // Discover converters
            converters = new Converter[requestedSchema.getFields().size()];
            int cont = 0;
            for (var f : requestedSchema.getFields()) {
                String name = f.getName();
                Repetition repetition = f.getRepetition();
                int index = mapper.getIndex(name);
                var recordComponent = mapper.getRecordComponent(name);
                if (f.isPrimitive()) {
                    PrimitiveType asPrimitive = f.asPrimitiveType();
                    PrimitiveTypeName type = asPrimitive.getPrimitiveTypeName();
                    switch (type) {
                    case INT32:
                        converters[cont] = buildFromInt32(constructor, index, recordComponent);
                        break;
                    case INT64:
                        converters[cont] = buildFromInt64Converter(constructor, index, recordComponent);
                        break;
                    case FLOAT:
                        converters[cont] = buildFromFloatConverter(constructor, index, recordComponent);
                        break;
                    case DOUBLE:
                        converters[cont] = buildFromDoubleConverter(constructor, index, recordComponent);
                        break;
                    case BOOLEAN:
                        converters[cont] = buildFromBooleanConverter(constructor, index, recordComponent);
                        break;
                    case BINARY:
                        converters[cont] = buildFromBinaryConverter(constructor, index, recordComponent, f);
                        break;
                    case INT96, FIXED_LEN_BYTE_ARRAY:
                        throw new RecordTypeConversionException(type + " deserialization not supported");
                    }
                } else {
                    throw new RecordTypeConversionException("GroupType deserialization not supported");
                }
                cont++;
            }
        }

        public Object getCurrentRecord() {
            return constructor.create();
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }

        @Override
        public void start() {
            Arrays.fill(constructor.c, null);

        }

        @Override
        public void end() {
            Object currentRecord = getCurrentRecord();
            parentConstructor.c[parentIndex] = currentRecord;
        }

    }

    public static class ConstructorParams {

        private final Constructor<?> constructor;
        public final Object[] c;

        public ConstructorParams(Class<?> recordClass) {
            constructor = findConstructor(recordClass);
            RecordComponent[] components = recordClass.getRecordComponents();
            c = new Object[components.length];
        }

        public Object create() {
            try {
                return constructor.newInstance(c);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private static Constructor<?> findConstructor(Class<?> recordClass) {
        Object[] componentsTypes = Stream.of(recordClass.getRecordComponents())
                .map(RecordComponent::getType)
                .toArray();
        Constructor<?>[] declaredConstructors = recordClass.getDeclaredConstructors();
        for (var c : declaredConstructors) {
            Class<?>[] parameterTypes = c.getParameterTypes();
            if (Arrays.equals(componentsTypes, parameterTypes, (c1, c2) -> c1.equals(c2) ? 0 : 1)) {
                c.setAccessible(true);
                return c;
            }
        }
        throw new RuntimeException(recordClass.getName() + " record has an invalid constructor");
    }
}

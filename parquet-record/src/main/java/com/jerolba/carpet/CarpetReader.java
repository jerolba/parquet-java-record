package com.jerolba.carpet;

import static com.jerolba.carpet.impl.Parametized.getParameterizedCollection;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromBinaryConverter;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromBooleanConverter;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromDoubleConverter;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromFloatConverter;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromInt32;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildFromInt64Converter;
import static com.jerolba.carpet.impl.read.PrimitiveListConverterFactory.listBuildFromBinaryConverter;
import static com.jerolba.carpet.impl.read.PrimitiveListConverterFactory.listBuildFromBooleanConverter;
import static com.jerolba.carpet.impl.read.PrimitiveListConverterFactory.listBuildFromDoubleConverter;
import static com.jerolba.carpet.impl.read.PrimitiveListConverterFactory.listBuildFromFloatConverter;
import static com.jerolba.carpet.impl.read.PrimitiveListConverterFactory.listBuildFromInt32;
import static com.jerolba.carpet.impl.read.PrimitiveListConverterFactory.listBuildFromInt64Converter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
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

import com.jerolba.carpet.impl.ParameterizedCollection;
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

        private final Converter[] converters;
        private final ConstructorParams constructor;

        public CarpetConverter(Class<?> groupClass, GroupType requestedSchema) {
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
                    converters[cont] = buildPrimitiveConverters(f, constructor, index, recordComponent);
                } else {
                    GroupType asGroupType = f.asGroupType();
                    LogicalTypeAnnotation logicalType = asGroupType.getLogicalTypeAnnotation();
                    if (logicalType == LogicalTypeAnnotation.listType()) {
                        var parameterized = getParameterizedCollection(recordComponent);
                        converters[cont] = new CarpetListConverter(parameterized, asGroupType,
                                value -> constructor.c[index] = value);
                    } else if (logicalType == LogicalTypeAnnotation.mapType()) {

                    } else {
                        Class<?> childClass = recordComponent.getType();
                        CarpetGroupConverter converter = new CarpetGroupConverter(childClass, asGroupType,
                                value -> constructor.c[index] = value);
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

        private final Converter[] converters;
        private final ConstructorParams constructor;
        private final Consumer<Object> groupConsumer;

        public CarpetGroupConverter(Class<?> groupClass, GroupType requestedSchema, Consumer<Object> groupConsumer) {
            this.groupConsumer = groupConsumer;
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
                    converters[cont] = buildPrimitiveConverters(f, constructor, index, recordComponent);
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
            groupConsumer.accept(currentRecord);
        }

    }

    static class CarpetListConverter extends GroupConverter {

        private final Consumer<Object> groupConsumer;
        private final Converter converter;
        private final ListHolder listHolder;
        private final AnnotatedLevels levels;

        public CarpetListConverter(ParameterizedCollection parameterized, GroupType requestedSchema,
                Consumer<Object> groupConsumer) {
            this.groupConsumer = groupConsumer;
            this.listHolder = new ListHolder();
            System.out.println(requestedSchema);

            // Discover converters
            List<Type> fields = requestedSchema.getFields();
            if (fields.size() > 1) {
                throw new RecordTypeConversionException(
                        requestedSchema.getName() + " LIST can not have more than one field");
            }
            Type listChild = fields.get(0);
            String name = listChild.getName();
            Repetition repetition = listChild.getRepetition();
            // Implement some logic to see if we have 2 or 3 level structures
            levels = AnnotatedLevels.THREE;
            if (levels == AnnotatedLevels.THREE) {
                converter = new CarpetListIntermediateConverter(parameterized, listChild.asGroupType(), listHolder);
            } else {
                converter = null;
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converter;
        }

        @Override
        public void start() {
            listHolder.start();
        }

        @Override
        public void end() {
            Object currentRecord = listHolder.create();
            groupConsumer.accept(currentRecord);
        }

    }

    public interface ListElementConsumer {
        void consume(Object value);
    }

    static class CarpetListIntermediateConverter extends GroupConverter implements ListElementConsumer {

        private final Converter converter;
        private final ListHolder listHolder;
        private Object elementValue;

        public CarpetListIntermediateConverter(ParameterizedCollection parameterized, GroupType requestedSchema,
                ListHolder listHolder) {
            System.out.println(requestedSchema);
            this.listHolder = listHolder;

            // Discover converters
            List<Type> fields = requestedSchema.getFields();
            if (fields.size() > 1) {
                throw new RecordTypeConversionException(
                        requestedSchema.getName() + " LIST child element can not have more than one field");
            }
            Type listElement = fields.get(0);
            String name = listElement.getName();
            if (listElement.isPrimitive()) {
                converter = buildPrimitiveListConverters(listElement, this, parameterized);
                return;
            }
            LogicalTypeAnnotation logicalType = listElement.getLogicalTypeAnnotation();
            if (logicalType == LogicalTypeAnnotation.listType() && parameterized.isCollection()) {
                var parameterized2 = parameterized.getParametizedAsCollection();
                converter = new CarpetListConverter(parameterized2, listElement.asGroupType(), this::consume);
                return;
            }
            if (parameterized.isMap()) {
                throw new RuntimeException("TODO MAP TYPE");
            }
            GroupType groupType = listElement.asGroupType();
            Class<?> listType = parameterized.getActualType();
            converter = new CarpetGroupConverter(listType, groupType, this::consume);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converter;
        }

        @Override
        public void start() {
            elementValue = null;
        }

        @Override
        public void end() {
            listHolder.add(elementValue);
        }

        @Override
        public void consume(Object value) {
            elementValue = value;
        }

    }

    private static Converter buildPrimitiveConverters(Type f, ConstructorParams constructor, int index,
            RecordComponent recordComponent) {

        PrimitiveType asPrimitive = f.asPrimitiveType();
        PrimitiveTypeName type = asPrimitive.getPrimitiveTypeName();
        switch (type) {
        case INT32:
            return buildFromInt32(constructor, index, recordComponent);
        case INT64:
            return buildFromInt64Converter(constructor, index, recordComponent);
        case FLOAT:
            return buildFromFloatConverter(constructor, index, recordComponent);
        case DOUBLE:
            return buildFromDoubleConverter(constructor, index, recordComponent);
        case BOOLEAN:
            return buildFromBooleanConverter(constructor, index, recordComponent);
        case BINARY:
            return buildFromBinaryConverter(constructor, index, recordComponent, f);
        case INT96, FIXED_LEN_BYTE_ARRAY:
            throw new RecordTypeConversionException(type + " deserialization not supported");
        }
        throw new RecordTypeConversionException(type + " deserialization not supported");
    }

    private static Converter buildPrimitiveListConverters(Type parquetField, ListElementConsumer listConsumer,
            ParameterizedCollection parameterized) {
        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        Class<?> listType = parameterized.getActualType();
        switch (type) {
        case INT32:
            return listBuildFromInt32(listConsumer, listType);
        case INT64:
            return listBuildFromInt64Converter(listConsumer, listType);
        case FLOAT:
            return listBuildFromFloatConverter(listConsumer, listType);
        case DOUBLE:
            return listBuildFromDoubleConverter(listConsumer, listType);
        case BOOLEAN:
            return listBuildFromBooleanConverter(listConsumer, listType);
        case BINARY:
            return listBuildFromBinaryConverter(listConsumer, listType, parquetField);
        case INT96, FIXED_LEN_BYTE_ARRAY:
            throw new RecordTypeConversionException(type + " deserialization not supported");
        }
        throw new RecordTypeConversionException(type + " deserialization not supported");
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

    public static class ListHolder {

        public List<Object> list;

        public void start() {
            list = new ArrayList<>();
        }

        public Object create() {
            return list;
        }

        public void add(Object value) {
            this.list.add(value);
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

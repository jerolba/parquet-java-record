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
package com.jerolba.avro.record;

import static com.jerolba.avro.record.AliasField.getFieldName;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.RECORD;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class AvroRecord2JavaRecord<T> {

    private static final Set<String> SIMPLE_MAPPER = Set.of("int", "java.lang.Integer", "long", "java.lang.Long",
            "double", "java.lang.Double", "float", "java.lang.Float", "boolean", "java.lang.Boolean");

    private final RecordInfo recordInfo;

    private record RecordInfo(Constructor<?> constructor, List<Function<GenericRecord, Object>> mappers) {
    }

    public AvroRecord2JavaRecord(Class<T> recordClass, Schema schema) {
        this.recordInfo = buildRecordInfo(recordClass, schema);
    }

    public T mapMainRecord(GenericRecord record) {
        return (T) map(recordInfo, record);
    }

    private RecordInfo buildRecordInfo(Class<?> recordClass, Schema schema) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException(recordClass.getName() + " is not a Java Record");
        }
        List<Function<GenericRecord, Object>> mappers = new ArrayList<>();
        for (RecordComponent recordComponent : recordClass.getRecordComponents()) {
            Field field = schema.getField(getFieldName(recordComponent));
            mappers.add(buildMapperForField(recordComponent, field));
        }
        return new RecordInfo(findConstructor(recordClass), mappers);
    }

    private Constructor<?> findConstructor(Class<?> recordClass) {
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

    private Function<GenericRecord, Object> buildMapperForField(RecordComponent recordComponent, Field avroField) {
        Class<?> attrJavaType = recordComponent.getType();
        if (avroField == null) {
            return getMissingParquetAttr(attrJavaType.getName());
        }
        var avroAttr = inspectField(avroField);
        if (avroAttr.isRecord()) {
            return getRecordTypeMapper(attrJavaType, avroAttr);
        }
        Type genericType = recordComponent.getGenericType();
        if (genericType instanceof TypeVariable<?>) {
            throw new RuntimeException("Generic type <" + genericType.toString() + "> not supported in records");
        } else if (genericType instanceof ParameterizedType) {
            Class<?> listType = getListCollectionType(recordComponent, avroField, avroAttr);
            return new ArrayMapper(listType, avroAttr.pos(), avroAttr.schema().getElementType()).getMapper();
        }
        int pos = avroField.pos();
        Function<Object, Object> mapper = getSimpleTypeMapper(attrJavaType);
        return record -> {
            Object v = record.get(pos);
            return v == null ? null : mapper.apply(v);
        };
    }

    private Function<GenericRecord, Object> getRecordTypeMapper(Class<?> javaType, AvroAtttribute avroAttr) {
        RecordInfo recursiveRecordInfo = buildRecordInfo(javaType, avroAttr.schema());
        int pos = avroAttr.pos();
        return parentRecord -> {
            GenericRecord childRecord = (GenericRecord) parentRecord.get(pos);
            return childRecord != null ? map(recursiveRecordInfo, childRecord) : null;
        };
    }

    private Class<?> getListCollectionType(RecordComponent recordComponent, Field field, AvroAtttribute avroAttr) {
        ParameterizedType paramType = (ParameterizedType) recordComponent.getGenericType();
        Class<?> parametizedClass = (Class<?>) paramType.getRawType();
        if (!avroAttr.isArray()) {
            throw new RuntimeException("Invalid parquet type " + field.schema().getType() + ", expected Array");
        }
        if (!Collection.class.isAssignableFrom(parametizedClass)) {
            throw new RuntimeException("Invalid collection type " + paramType.getRawType());
        }
        Type listType = paramType.getActualTypeArguments()[0];
        if (!(listType instanceof Class<?>)) {
            throw new RuntimeException("Invalid type " + parametizedClass + " as " + listType);
        }
        return (Class<?>) listType;
    }

    record AvroAtttribute(boolean nullable, Schema schema, int pos) {

        boolean isRecord() {
            return schema.getType() == RECORD;
        }

        boolean isArray() {
            return schema.getType() == ARRAY;
        }
    }

    private AvroAtttribute inspectField(Field avroField) {
        if (!avroField.schema().isNullable()) {
            return new AvroAtttribute(false, avroField.schema(), avroField.pos());
        }
        for (Schema schema : avroField.schema().getTypes()) {
            if (!schema.isNullable()) {
                return new AvroAtttribute(true, schema, avroField.pos());
            }
        }
        return null;
    }

    private Object map(RecordInfo recordInfo, GenericRecord record) {
        List<Function<GenericRecord, Object>> mappers = recordInfo.mappers();
        Object[] values = new Object[mappers.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = mappers.get(i).apply(record);
        }
        try {
            return recordInfo.constructor().newInstance(values);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private Function<Object, Object> getSimpleTypeMapper(Class<?> type) {
        if (type.equals(java.lang.String.class)) {
            return Object::toString;
        }
        if (SIMPLE_MAPPER.contains(type.getName())) {
            return v -> v;
        }
        if ("byte".equals(type.getName()) || "java.lang.Byte".equals(type.getName())) {
            return v -> ((Number) v).byteValue();
        }
        if ("short".equals(type.getName()) || "java.lang.Short".equals(type.getName())) {
            return v -> ((Number) v).shortValue();
        }
        if (type.isEnum()) {
            Class<? extends Enum> asEnum = type.asSubclass(Enum.class);
            return v -> Enum.valueOf(asEnum, v.toString());
        }
        throw new RuntimeException(type + " type not supported");
    }

    private class ArrayMapper {

        private final Class<?> listType;
        private final Schema schema;
        private final int pos;

        ArrayMapper(Class<?> listType, int pos, Schema schema) {
            this.listType = listType;
            this.schema = schema;
            this.pos = pos;
        }

        Function<GenericRecord, Object> getMapper() {
            if (schema.getType() == RECORD) {
                RecordInfo recursiveRecordInfo = buildRecordInfo(listType, schema);
                return getCollectionMapper(e -> map(recursiveRecordInfo, (GenericRecord) e));
            }
            return getArraySimpleTypeMapper();
        }

        private Function<GenericRecord, Object> getArraySimpleTypeMapper() {
//            // TODO: check compatibility between avroType and type
            var avroType = schema.getType();
            Function<Object, Object> simpleTypeMapper = getSimpleTypeMapper(listType);
            return getCollectionMapper(simpleTypeMapper);
        }

        private Function<GenericRecord, Object> getCollectionMapper(Function<Object, Object> elementMapper) {
            return parentRecord -> {
                Object arrayget = parentRecord.get(pos);
                if (arrayget == null) {
                    return null;
                }
                Collection<?> array = (Collection<?>) arrayget;
                List<Object> res = new ArrayList<>(array.size());
                for (var e : array) {
                    res.add(e == null ? null : elementMapper.apply(e));
                }
                return res;
            };
        }

    }

    private Function<GenericRecord, Object> getMissingParquetAttr(String type) {
        switch (type) {
        case "java.lang.String":
            return r -> null;
        case "byte", "java.lang.Byte":
            return r -> (byte) 0;
        case "short", "java.lang.Short":
            return r -> (short) 0;
        case "int", "java.lang.Integer":
            return r -> 0;
        case "long", "java.lang.Long":
            return r -> 0L;
        case "double", "java.lang.Double":
            return r -> 0.0;
        case "float", "java.lang.Float":
            return r -> 0.0F;
        case "boolean", "java.lang.Boolean":
            return r -> false;
        }
        return r -> null;
    }

}

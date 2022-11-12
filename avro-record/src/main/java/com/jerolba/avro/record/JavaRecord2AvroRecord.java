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

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericRecord;

public class JavaRecord2AvroRecord<T> {

    private final RecordInfo recordInfo;

    record FieldMap(Function<Object, Object> mapper, int pos) {

        FieldMap(Field avroField, Function<Object, Object> map) {
            this(map, avroField.pos());
        }
    }

    record RecordInfo(Schema schema, List<FieldMap> mappers) {
    }

    public JavaRecord2AvroRecord(Class<T> recordClass, Schema schema) {
        try {
            this.recordInfo = buildRecordInfo(recordClass, schema);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private RecordInfo buildRecordInfo(Class<?> recordClass, Schema schema) throws Throwable {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException(recordClass.getName() + " is not a Java Record");
        }
        List<FieldMap> mappers = new ArrayList<>();
        for (RecordComponent recordComponent : recordClass.getRecordComponents()) {
            Field field = schema.getField(getFieldName(recordComponent));
            mappers.add(new SimpleMapperBuilder(field, recordClass, recordComponent).buildMapperForField());
        }
        return new RecordInfo(schema, mappers);
    }

    public GenericRecord mapRecord(T t) {
        return map(recordInfo, t);
    }

    private GenericRecord map(RecordInfo recordInfo, Object obj) {
        if (obj == null) {
            return null;
        }
        GenericData.Record record = new GenericData.Record(recordInfo.schema());
        for (var fieldMapper : recordInfo.mappers()) {
            Object value = fieldMapper.mapper().apply(obj);
            if (value != null) {
                record.put(fieldMapper.pos(), value);
            }
        }
        return record;
    }

    private static final Set<String> SIMPLE_MAPPER = Set.of("int", "java.lang.Integer", "long", "java.lang.Long",
            "double", "java.lang.Double", "float", "java.lang.Float", "boolean", "java.lang.Boolean",
            "java.lang.String");

    private class SimpleMapperBuilder {

        private final Field avroField;
        private final Class<?> targetClass;
        private final RecordComponent recordComponent;
        private final Function<Object, Object> recordAccessor;

        SimpleMapperBuilder(Field avroField, Class<?> targetClass, RecordComponent recordComponent) throws Throwable {
            this.avroField = avroField;
            this.targetClass = targetClass;
            this.recordComponent = recordComponent;
            this.recordAccessor = recordAccessor(targetClass, recordComponent);
        }

        public FieldMap buildMapperForField() throws Throwable {
            Class<?> attrJavaType = recordComponent.getType();
            if (attrJavaType.isRecord()) {
                return getRecordTypeMapper();
            }
            if (recordComponent.getGenericType() instanceof ParameterizedType) {
                return new CollectionMapperBuilder(avroField, targetClass, recordComponent).getMapper();
            }
            if (SIMPLE_MAPPER.contains(attrJavaType.getName())) {
                return new FieldMap(avroField, recordAccessor);
            }
            if ("short".equals(attrJavaType.getName()) || "java.lang.Short".equals(attrJavaType.getName())) {
                return new FieldMap(avroField, value -> {
                    Object v = recordAccessor.apply(value);
                    if (v == null) {
                        return null;
                    }
                    return ((Short) v).intValue();
                });
            }
            if ("byte".equals(attrJavaType.getName()) || "java.lang.Byte".equals(attrJavaType.getName())) {
                return new FieldMap(avroField, value -> {
                    Object v = recordAccessor.apply(value);
                    if (v == null) {
                        return null;
                    }
                    return ((Byte) v).intValue();
                });
            }
            if (attrJavaType.isEnum()) {
                return getEnumMapper();
            }
            throw new RuntimeException(attrJavaType + " type not supported");
        }

        private FieldMap getRecordTypeMapper() throws Throwable {
            Schema childSchema = fieldNotNullSchema(avroField);
            RecordInfo childRecordInfo = buildRecordInfo(recordComponent.getType(), childSchema);
            return new FieldMap(avroField, record -> {
                Object value = recordAccessor.apply(record);
                return map(childRecordInfo, value);
            });
        }

        private FieldMap getEnumMapper() {
            Schema schema = fieldNotNullSchema(avroField);
            EnumsValues enumValues = new EnumsValues(schema, recordComponent.getType());
            return new FieldMap(avroField, record -> enumValues.getValue(recordAccessor.apply(record)));
        }
    }

    private class CollectionMapperBuilder {

        private final Field avroField;
        private final RecordComponent recordComponent;
        private final Function<Object, Object> recordAccessor;

        CollectionMapperBuilder(Field avroField, Class<?> targetClass, RecordComponent recordComponent)
                throws Throwable {
            this.avroField = avroField;
            this.recordComponent = recordComponent;
            this.recordAccessor = recordAccessor(targetClass, recordComponent);
        }

        private FieldMap getMapper() throws Throwable {
            ParameterizedType paramType = (ParameterizedType) recordComponent.getGenericType();
            Class<?> listType = (Class<?>) paramType.getActualTypeArguments()[0];
            if (listType.isRecord()) {
                return collectionRecordMapper(listType);
            }
            if (SIMPLE_MAPPER.contains(listType.getTypeName())) {
                return collectionSimpleMapper();
            }
            if (listType.isEnum()) {
                return collectionEnumMapper(listType);
            }
            throw new RuntimeException("Unsuported type in collection: " + listType.getName());
        }

        private FieldMap collectionSimpleMapper() {
            return new FieldMap(avroField, collectionField -> {
                Object v = recordAccessor.apply(collectionField);
                if (v == null) {
                    return null;
                }
                return new ArrayList<>((Collection<?>) v);
            });
        }

        private FieldMap collectionEnumMapper(Class<?> listType) {
            Schema arrayType = fieldNotNullSchema(avroField).getElementType();
            EnumsValues enumValues = new EnumsValues(arrayType, listType);
            return new FieldMap(avroField, collectionField -> {
                Object v = recordAccessor.apply(collectionField);
                if (v == null) {
                    return null;
                }
                List<Object> array = new ArrayList<>();
                for (Object enumValue : (Collection<?>) v) {
                    array.add(enumValues.getValue(enumValue));
                }
                return array;
            });
        }

        private FieldMap collectionRecordMapper(Class<?> listType) throws Throwable {
            Schema arrayType = fieldNotNullSchema(avroField).getElementType();
            RecordInfo childRecordInfo = buildRecordInfo(listType, arrayType);
            return new FieldMap(avroField, record -> {
                Object v = recordAccessor.apply(record);
                if (v == null) {
                    return null;
                }
                List<Object> array = new ArrayList<>();
                for (Object recordValue : (Collection<?>) v) {
                    array.add(map(childRecordInfo, recordValue));
                }
                return array;
            });
        }
    }

    private Schema fieldNotNullSchema(Field avroField) {
        Schema schema = avroField.schema();
        if (schema.isUnion()) {
            return avroField.schema().getTypes().stream().filter(t -> !t.isNullable()).findFirst().get();
        }
        return schema;
    }

    private static class EnumsValues {

        private final EnumSymbol[] values;

        EnumsValues(Schema schema, Class<?> enumType) {
            Object[] enums = enumType.getEnumConstants();
            values = new EnumSymbol[enums.length];
            for (int i = 0; i < enums.length; i++) {
                values[i] = new EnumSymbol(schema, enums[i].toString());
            }
        }

        public Object getValue(Object v) {
            return v == null ? null : values[((Enum<?>) v).ordinal()];
        }
    }

    private static Function<Object, Object> recordAccessor(Class<?> targetClass, RecordComponent recordComponent)
            throws Throwable {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodHandle findVirtual = lookup.findVirtual(targetClass, recordComponent.getName(),
                MethodType.methodType(recordComponent.getType()));
        CallSite site = LambdaMetafactory.metafactory(lookup,
                "apply",
                MethodType.methodType(Function.class),
                MethodType.methodType(Object.class, Object.class),
                findVirtual,
                MethodType.methodType(recordComponent.getType(), targetClass));
        return (Function<Object, Object>) site.getTarget().invokeExact();
    }

}
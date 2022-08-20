package com.jerolba.avro.record;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;

public class JavaRecord2AvroRecord<T> {

    private final RecordInfo recordInfo;

    record FieldMap(Function<Object, Object> map, int pos) {

        FieldMap(Field avroField, Function<Object, Object> map) {
            this(map, avroField.pos());
        }
    }

    record RecordInfo(Schema schema, List<FieldMap> mappers) {
    }

    public JavaRecord2AvroRecord(Class<T> recordClass, Schema schema) {
        this.recordInfo = buildRecordInfo(recordClass, schema);
    }

    private RecordInfo buildRecordInfo(Class<?> recordClass, Schema schema) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException(recordClass.getName() + " is not a Java Record");
        }
        List<FieldMap> mappers = new ArrayList<>();
        for (RecordComponent recordComponent : recordClass.getRecordComponents()) {
            // Review adding aliasing
            Field field = schema.getField(recordComponent.getName());
            mappers.add(new SimpleMapper(field, recordComponent).buildMapperForField());
        }
        return new RecordInfo(schema, mappers);
    }

    public GenericRecord mapRecord(T t) {
        return map(recordInfo, t);
    }

    private GenericRecord map(RecordInfo recordInfo, Object obj) {
        GenericData.Record record = new GenericData.Record(recordInfo.schema());
        for (var fm : recordInfo.mappers()) {
            Object value = fm.map().apply(obj);
            if (value != null) {
                record.put(fm.pos(), value);
            }
        }
        return record;
    }

    private static final Set<String> SIMPLE_MAPPER = Set.of("int", "java.lang.Integer", "long", "java.lang.Long",
            "double", "java.lang.Double", "float", "java.lang.Float", "boolean", "java.lang.Boolean", "java.lang.String");

    private class SimpleMapper {

        private final Field avroField;
        private final RecordComponent recordComponent;
        private final Method accessor;

        public SimpleMapper(Field avroField, RecordComponent recordComponent) {
            this.avroField = avroField;
            this.recordComponent = recordComponent;
            this.accessor = recordComponent.getAccessor();
        }

        public FieldMap buildMapperForField() {
            Class<?> attrJavaType = recordComponent.getType();
            if (attrJavaType.isRecord()) {
                return getRecordTypeMapper();
            }
            if (recordComponent.getGenericType() instanceof ParameterizedType) {
                return new CollectionMapper(avroField, recordComponent).getMapper();
            }
            if (SIMPLE_MAPPER.contains(attrJavaType.getName())) {
                return new FieldMap(avroField, record -> safeInvoke(accessor, record));
            }
            if (attrJavaType.isEnum()) {
                return getEnumMapper();
            }
            throw new RuntimeException(attrJavaType + " type not supported");
        }

        private FieldMap getRecordTypeMapper() {
            Schema childSchema = fieldNotNullSchema(avroField);
            RecordInfo childRecordInfo = buildRecordInfo(recordComponent.getType(), childSchema);
            return new FieldMap(avroField, record -> {
                Object value = safeInvoke(accessor, record);
                if (value != null) {
                    return map(childRecordInfo, value);
                }
                return null;
            });
        }

        private FieldMap getEnumMapper() {
            Schema schema = fieldNotNullSchema(avroField);
            return new FieldMap(avroField, record -> {
                Object v = safeInvoke(accessor, record);
                return v == null ? null : new EnumValue((Enum<?>) v, schema);
            });
        }
    }

    private class CollectionMapper {

        private final Field avroField;
        private final RecordComponent recordComponent;
        private final Method accessor;

        public CollectionMapper(Field avroField, RecordComponent recordComponent) {
            this.avroField = avroField;
            this.recordComponent = recordComponent;
            this.accessor = recordComponent.getAccessor();
        }

        public FieldMap getMapper() {
            ParameterizedType paramType = (ParameterizedType) recordComponent.getGenericType();
            Class<?> listType = (Class<?>) paramType.getActualTypeArguments()[0];
            if (listType.isRecord()) {
                return collectionRecordMapper(listType);
            }
            if (SIMPLE_MAPPER.contains(listType.getTypeName())) {
                return collectionSimpleMapper();
            }
            if (listType.isEnum()) {
                return collectionEnumMapper();
            }
            throw new RuntimeException("Unsuported type in collection: " + listType.getName());
        }

        private FieldMap collectionSimpleMapper() {
            return new FieldMap(avroField, collectionField -> {
                Object v = safeInvoke(accessor, collectionField);
                if (v == null) {
                    return null;
                }
                return new ArrayList<>((Collection<?>) v);
            });
        }

        private FieldMap collectionEnumMapper() {
            Schema arrayType = fieldNotNullSchema(avroField).getElementType();
            return new FieldMap(avroField, collectionField -> {
                Object v = safeInvoke(accessor, collectionField);
                if (v == null) {
                    return null;
                }
                List<Object> array = new ArrayList<>();
                for (Object enumValue : (Collection<?>) v) {
                    if (enumValue != null) {
                        array.add(new EnumValue((Enum<?>) enumValue, arrayType));
                    } else {
                        array.add(null);
                    }
                }
                return array;
            });
        }

        private FieldMap collectionRecordMapper(Class<?> listType) {
            Schema arrayType = fieldNotNullSchema(avroField).getElementType();
            RecordInfo childRecordInfo = buildRecordInfo(listType, arrayType);
            return new FieldMap(avroField, record -> {
                Object v = safeInvoke(accessor, record);
                if (v == null) {
                    return null;
                }
                List<Object> array = new ArrayList<>();
                for (Object recordValue : (Collection<?>) v) {
                    if (recordValue != null) {
                        array.add(map(childRecordInfo, recordValue));
                    } else {
                        array.add(null);
                    }
                }
                return array;
            });
        }
    }

    private Schema fieldNotNullSchema(Field avroField) {
        return avroField.schema().getTypes().stream().filter(t -> !t.isNullable()).findFirst().get();
    }

    private Object safeInvoke(Method accessor, Object record) {
        try {
            return accessor.invoke(record);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static class EnumValue implements GenericEnumSymbol<EnumValue> {

        private Enum<?> value;
        private Schema schema;

        public EnumValue(Enum<?> value, Schema schema) {
            this.value = value;
            this.schema = schema;
        }

        @Override
        public Schema getSchema() {
            return schema;
        }

        @Override
        public int compareTo(EnumValue o) {
            return Integer.compare(value.ordinal(), o.value.ordinal());
        }

        @Override
        public String toString() {
            return value.name();
        }

    }

}
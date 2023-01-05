package com.jerolba.carpet;

import static com.jerolba.carpet.AliasField.getFieldName;
import static com.jerolba.carpet.ParametizedObject.getCollectionClass;
import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

public class CarpetRecordWriter {

    private final RecordConsumer recordConsumer;
    private final Class<?> recordClass;
    private final CarpetConfiguration carpetConfiguration;

    private final List<FieldWriter> fieldWriters = new ArrayList<>();

    public CarpetRecordWriter(RecordConsumer recordConsumer, Class<?> recordClass,
            CarpetConfiguration carpetConfiguration) throws Throwable {
        this.recordConsumer = recordConsumer;
        this.recordClass = recordClass;
        this.carpetConfiguration = carpetConfiguration;

        // Preconditions: All fields are writable
        int idx = 0;
        for (RecordComponent attr : recordClass.getRecordComponents()) {
            String fieldName = getFieldName(attr);

            Class<?> type = attr.getType();
            String typeName = type.getName();
            FieldWriter writer = null;
            RecordField f = new RecordField(recordClass, attr, fieldName, idx);

            writer = buildBasicTypeWriter(typeName, type, f);

            if (writer == null) {
                if (type.isRecord()) {
                    var recordWriter = new CarpetRecordWriter(recordConsumer, type, carpetConfiguration);
                    writer = new RecordFieldWriter(f, recordWriter);
                } else if (Collection.class.isAssignableFrom(type)) {
                    ParametizedObject collectionClass = getCollectionClass(attr);
                    writer = createCollectionWriter(collectionClass, f);
                } else {
                    System.out.println(typeName + " can not be serialized");
                    // throw new RuntimeException(typeName + " can not be serialized");
                }
            }
            fieldWriters.add(writer);
            idx++;
        }
    }

    private FieldWriter createCollectionWriter(ParametizedObject collectionClass, RecordField f) throws Throwable {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> createCollectionWriterOneLevel(collectionClass, f);
        case TWO -> createCollectionWriterTwoLevel(collectionClass, f);
        case THREE -> createCollectionWriterThreeLevel(collectionClass, f);
        };
    }

    private FieldWriter createCollectionWriterOneLevel(ParametizedObject parametized, RecordField f) throws Throwable {
        Class<?> type = parametized.getActualType();
        String typeName = type.getName();
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            elemConsumer = (consumer, v) -> consumer.addInteger((Integer) v);
        } else if (typeName.equals("java.lang.String")) {
            elemConsumer = (consumer, v) -> consumer.addBinary(Binary.fromString((String) v));
        } else if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            elemConsumer = (consumer, v) -> consumer.addBoolean((Boolean) v);
        } else if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            elemConsumer = (consumer, v) -> consumer.addLong((Long) v);
        } else if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            elemConsumer = (consumer, v) -> consumer.addDouble((Double) v);
        } else if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            elemConsumer = (consumer, v) -> consumer.addFloat((Float) v);
        } else if (typeName.equals("short") || typeName.equals("java.lang.Short") ||
                typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            elemConsumer = (consumer, v) -> consumer.addInteger(((Number) v).intValue());
        } else if (type.isEnum()) {
            EnumsValues enumValues = new EnumsValues(type);
            elemConsumer = (consumer, v) -> consumer.addBinary(enumValues.getValue(v));
        } else if (type.isRecord()) {
            CarpetRecordWriter recordWriter = new CarpetRecordWriter(recordConsumer, type, carpetConfiguration);
            elemConsumer = (consumer, v) -> {
                consumer.startGroup();
                recordWriter.write(v);
                consumer.endGroup();
            };
        } else if (Collection.class.isAssignableFrom(type)) {
            throw new RecordTypeConversionException(
                    "Nested collection in a collection is not supported in single level structure codification");
        }
        if (elemConsumer != null) {
            return new OneLevelCollectionFieldWriter(f, elemConsumer);
        }
        throw new RecordTypeConversionException("Unsuported type in collection");
    }

    private class OneLevelCollectionFieldWriter extends FieldWriter {

        private final BiConsumer<RecordConsumer, Object> consumer;

        public OneLevelCollectionFieldWriter(RecordField recordField, BiConsumer<RecordConsumer, Object> consumer)
                throws Throwable {
            super(recordField);
            this.consumer = consumer;
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                Collection<?> coll = (Collection<?>) value;
                for (var v : coll) {
                    consumer.accept(recordConsumer, v);
                }
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private FieldWriter createCollectionWriterTwoLevel(ParametizedObject parametized, RecordField f) {
        return null;
    }

    private FieldWriter createCollectionWriterThreeLevel(ParametizedObject parametized, RecordField f)
            throws Throwable {
        return null;
    }

    private FieldWriter buildBasicTypeWriter(String typeName, Class<?> type, RecordField f) throws Throwable {
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new IntegerFieldWriter(f);
        } else if (typeName.equals("java.lang.String")) {
            return new StringFieldWriter(f);
        } else if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return new BooleanFieldWriter(f);
        } else if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new LongFieldWriter(f);
        } else if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new DoubleFieldWriter(f);
        } else if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new FloatFieldWriter(f);
        } else if (typeName.equals("short") || typeName.equals("java.lang.Short") ||
                typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new IntegerCompatibleFieldWriter(f);
        } else if (type.isEnum()) {
            return new EnumFieldWriter(f, type);
        }
        return null;
    }

    public void write(Object record) {
        for (var fieldWriter : fieldWriters) {
            fieldWriter.writeField(record);
        }
    }

    private abstract class FieldWriter {

        protected final RecordField recordField;
        protected final Function<Object, Object> accesor;

        public FieldWriter(RecordField recordField) throws Throwable {
            this.recordField = recordField;
            this.accesor = recordAccessor(recordField.targetClass, recordField.recordComponent);
        }

        abstract void writeField(Object object);

    }

    private class IntegerFieldWriter extends FieldWriter {

        public IntegerFieldWriter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addInteger((Integer) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class IntegerCompatibleFieldWriter extends FieldWriter {

        public IntegerCompatibleFieldWriter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addInteger(((Number) value).intValue());
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class LongFieldWriter extends FieldWriter {

        public LongFieldWriter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addLong((Long) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class BooleanFieldWriter extends FieldWriter {

        public BooleanFieldWriter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addBoolean((Boolean) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class FloatFieldWriter extends FieldWriter {

        public FloatFieldWriter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addFloat((Float) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class DoubleFieldWriter extends FieldWriter {

        public DoubleFieldWriter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addDouble((Double) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class StringFieldWriter extends FieldWriter {

        public StringFieldWriter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addBinary(Binary.fromString((String) value));
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class EnumFieldWriter extends FieldWriter {

        private final EnumsValues values;

        public EnumFieldWriter(RecordField recordField, Class<?> enumClass) throws Throwable {
            super(recordField);
            values = new EnumsValues(enumClass);
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addBinary(values.getValue(value));
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }

    }

    private static class EnumsValues {

        private final Binary[] values;

        EnumsValues(Class<?> enumType) {
            Object[] enums = enumType.getEnumConstants();
            values = new Binary[enums.length];
            for (int i = 0; i < enums.length; i++) {
                values[i] = Binary.fromString(((Enum<?>) enums[i]).name());
            }
        }

        public Binary getValue(Object v) {
            return values[((Enum<?>) v).ordinal()];
        }

    }

    private class RecordFieldWriter extends FieldWriter {

        private final CarpetRecordWriter writer;

        public RecordFieldWriter(RecordField recordField, CarpetRecordWriter writer) throws Throwable {
            super(recordField);
            this.writer = writer;
        }

        @Override
        void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.startGroup();
                writer.write(value);
                recordConsumer.endGroup();
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    record RecordField(Class<?> targetClass, RecordComponent recordComponent, String fieldName, int idx) {

    }

    private static Function<Object, Object> recordAccessor(Class<?> targetClass, RecordComponent recordComponent)
            throws Throwable {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodHandle findVirtual = lookup.findVirtual(targetClass, recordComponent.getName(),
                methodType(recordComponent.getType()));
        CallSite site = LambdaMetafactory.metafactory(lookup,
                "apply",
                methodType(Function.class),
                methodType(Object.class, Object.class),
                findVirtual,
                methodType(recordComponent.getType(), targetClass));
        return (Function<Object, Object>) site.getTarget().invokeExact();
    }

}

package com.jerolba.carpet;

import static com.jerolba.carpet.AliasField.getFieldName;
import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

public class CarpetRecordWriter {

    private final RecordConsumer recordConsumer;
    private final Class<?> recordClass;

    private final List<FieldWriter> writers = new ArrayList<>();

    public CarpetRecordWriter(RecordConsumer recordConsumer, Class<?> recordClass) throws Throwable {
        this.recordConsumer = recordConsumer;
        this.recordClass = recordClass;

        // Preconditions: All fields are writable
        int idx = 0;
        for (RecordComponent recordComponent : recordClass.getRecordComponents()) {
            String fieldName = getFieldName(recordComponent);

            Class<?> type = recordComponent.getType();
            String typeName = type.getName();
            FieldWriter writer = null;
            RecordField f = new RecordField(recordClass, recordComponent, fieldName, idx);

            if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
                writer = new IntegerFieldWriter(f);
            } else if (typeName.equals("java.lang.String")) {
                writer = new StringFieldWriter(f);
            } else if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
                writer = new BooleanFieldWriter(f);
            } else if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
                writer = new LongFieldWriter(f);
            } else if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
                writer = new DoubleFieldWriter(f);
            } else if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
                writer = new FloatFieldWriter(f);
            } else if (typeName.equals("short") || typeName.equals("java.lang.Short") ||
                    typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
                writer = new IntegerCompatibleFieldWriter(f);
            } else if (type.isEnum()) {
                writer = new EnumFieldWriter(f, type);
            } else if (type.isRecord()) {
                var recordWriter = new CarpetRecordWriter(recordConsumer, type);
                writer = new RecordFieldWriter(f, recordWriter);
            }
            writers.add(writer);
            idx++;
        }
    }

    public void write(Object record) {
        for (var writter : writers) {
            writter.writeField(record);
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

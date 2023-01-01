package com.jerolba.tarima;

import static com.jerolba.tarima.AliasField.getFieldName;
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

public class TarimaRecordWriter<T> {

    private final RecordConsumer recordConsumer;
    private final Class<T> recordClass;

    private final List<FieldWritter> writers = new ArrayList<>();

    public TarimaRecordWriter(RecordConsumer recordConsumer, Class<T> recordClass) throws Throwable {
        this.recordConsumer = recordConsumer;
        this.recordClass = recordClass;

        // Preconditions: All fields are writable
        int idx = 0;
        for (RecordComponent recordComponent : recordClass.getRecordComponents()) {
            String fieldName = getFieldName(recordComponent);

            Class<?> type = recordComponent.getType();
            String typeName = type.getName();
            FieldWritter writer = null;
            RecordField f = new RecordField(recordClass, recordComponent, fieldName, idx);
            if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
                writer = new IntegerFieldWritter(f);
            } else if (typeName.equals("java.lang.String")) {
                writer = new StringFieldWritter(f);
            } else if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
                writer = new BooleanFieldWritter(f);
            } else if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
                writer = new LongFieldWritter(f);
            } else if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
                writer = new DoubleFieldWritter(f);
            } else if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
                writer = new FloatFieldWritter(f);
            } else if (typeName.equals("short") || typeName.equals("java.lang.Short") ||
                    typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
                writer = new IntegerCompatibleFieldWritter(f);
            }
            writers.add(writer);
            idx++;
        }
    }

    public void write(T record) {
        recordConsumer.startMessage();
        for (var writter : writers) {
            writter.writeField(record);
        }
        recordConsumer.endMessage();
    }

    private abstract class FieldWritter {

        protected final RecordField recordField;
        protected final Function<Object, Object> accesor;

        public FieldWritter(RecordField recordField) throws Throwable {
            this.recordField = recordField;
            this.accesor = recordAccessor(recordField.targetClass, recordField.recordComponent);
        }

        abstract void writeField(T object);

    }

    private class IntegerFieldWritter extends FieldWritter {

        public IntegerFieldWritter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(T object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addInteger((Integer) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class IntegerCompatibleFieldWritter extends FieldWritter {

        public IntegerCompatibleFieldWritter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(T object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addInteger(((Number) value).intValue());
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class LongFieldWritter extends FieldWritter {

        public LongFieldWritter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(T object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addLong((Long) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class BooleanFieldWritter extends FieldWritter {

        public BooleanFieldWritter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(T object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addBoolean((Boolean) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class FloatFieldWritter extends FieldWritter {

        public FloatFieldWritter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(T object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addFloat((Float) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class DoubleFieldWritter extends FieldWritter {

        public DoubleFieldWritter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(T object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addDouble((Double) value);
                recordConsumer.endField(recordField.fieldName, recordField.idx);
            }
        }
    }

    private class StringFieldWritter extends FieldWritter {

        public StringFieldWritter(RecordField recordField) throws Throwable {
            super(recordField);
        }

        @Override
        void writeField(T object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName, recordField.idx);
                recordConsumer.addBinary(Binary.fromString((String) value));
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

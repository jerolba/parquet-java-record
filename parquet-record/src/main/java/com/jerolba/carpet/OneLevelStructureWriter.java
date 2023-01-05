package com.jerolba.carpet;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

class OneLevelStructureWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetConfiguration carpetConfiguration;

    public OneLevelStructureWriter(RecordConsumer recordConsumer, CarpetConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;
    }

    public Consumer<Object> createCollectionWriterOneLevel(ParametizedObject parametized, RecordField f)
            throws Throwable {
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
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                Collection<?> coll = (Collection<?>) value;
                for (var v : coll) {
                    consumer.accept(recordConsumer, v);
                }
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

}

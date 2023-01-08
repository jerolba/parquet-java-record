package com.jerolba.carpet;

import static com.jerolba.carpet.SimpleCollectionItemConsumerFactory.buildSimpleElementConsumer;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.parquet.io.api.RecordConsumer;

class OneLevelStructureWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetConfiguration carpetConfiguration;

    public OneLevelStructureWriter(RecordConsumer recordConsumer, CarpetConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;
    }

    public Consumer<Object> createCollectionWriter(ParameterizedCollection parametized, RecordField field)
            throws Throwable {
        if (parametized.isCollection()) {
            throw new RecordTypeConversionException(
                    "Nested collection in a collection is not supported in single level structure codification");
        }
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized.isMap()) {
            ParameterizedMap parametizedChild = parametized.getParametizedAsMap();
            var mapStructWriter = new MapStructureWriter(recordConsumer, carpetConfiguration);
            Consumer<Object> childWriter = mapStructWriter.createMapWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> type = parametized.getActualType();
            elemConsumer = buildSimpleElementConsumer(type, recordConsumer, carpetConfiguration);
        }
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        return new OneLevelCollectionFieldWriter(field, elemConsumer);
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

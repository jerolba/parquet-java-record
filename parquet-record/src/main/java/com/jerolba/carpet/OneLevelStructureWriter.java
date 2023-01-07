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

    public Consumer<Object> createCollectionWriter(ParameterizedCollection parametized, RecordField f)
            throws Throwable {
        Class<?> type = parametized.getActualType();
        var elemConsumer = buildSimpleElementConsumer(type, recordConsumer, carpetConfiguration);
        if (elemConsumer != null) {
            return new OneLevelCollectionFieldWriter(f, elemConsumer);
        }
        if (Collection.class.isAssignableFrom(type)) {
            throw new RecordTypeConversionException(
                    "Nested collection in a collection is not supported in single level structure codification");
        }
        throw new RecordTypeConversionException("Unsuported type in collection " + type);
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

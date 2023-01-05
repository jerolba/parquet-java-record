package com.jerolba.carpet;

import java.util.function.Consumer;
import java.util.function.Function;

abstract class FieldWriter implements Consumer<Object> {

    protected final RecordField recordField;
    protected final Function<Object, Object> accesor;

    public FieldWriter(RecordField recordField) throws Throwable {
        this.recordField = recordField;
        this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
    }

    abstract void writeField(Object object);

    @Override
    public void accept(Object object) {
        writeField(object);
    }
}

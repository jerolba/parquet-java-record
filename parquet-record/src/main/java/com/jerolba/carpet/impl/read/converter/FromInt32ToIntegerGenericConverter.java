package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class FromInt32ToIntegerGenericConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public FromInt32ToIntegerGenericConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addInt(int value) {
        listConsumer.accept(value);
    }

}

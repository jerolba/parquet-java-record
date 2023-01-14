package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class FromFloatToDoubleGenericConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public FromFloatToDoubleGenericConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addFloat(float value) {
        listConsumer.accept((double) value);
    }

}

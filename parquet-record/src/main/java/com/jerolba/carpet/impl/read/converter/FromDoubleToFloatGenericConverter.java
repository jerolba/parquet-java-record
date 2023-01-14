package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class FromDoubleToFloatGenericConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public FromDoubleToFloatGenericConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addDouble(double value) {
        listConsumer.accept((float) value);
    }

}

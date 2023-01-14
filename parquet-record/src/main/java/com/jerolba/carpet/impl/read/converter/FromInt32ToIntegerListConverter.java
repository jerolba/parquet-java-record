package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class FromInt32ToIntegerListConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public FromInt32ToIntegerListConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addInt(int value) {
        listConsumer.accept(value);
    }

}

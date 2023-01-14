package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class BooleanListConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public BooleanListConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addBoolean(boolean value) {
        listConsumer.accept(value);
    }

}

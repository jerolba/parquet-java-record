package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class FromInt64ToShortListConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public FromInt64ToShortListConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addLong(long value) {
        listConsumer.accept((short) value);
    }

}

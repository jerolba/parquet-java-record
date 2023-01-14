package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class BooleanListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public BooleanListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addBoolean(boolean value) {
        listConsumer.consume(value);
    }

}

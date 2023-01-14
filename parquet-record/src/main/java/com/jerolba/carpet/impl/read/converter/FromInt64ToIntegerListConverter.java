package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class FromInt64ToIntegerListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public FromInt64ToIntegerListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addLong(long value) {
        listConsumer.consume((int) value);
    }

}

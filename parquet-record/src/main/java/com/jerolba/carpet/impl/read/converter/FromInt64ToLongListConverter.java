package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class FromInt64ToLongListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public FromInt64ToLongListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addLong(long value) {
        listConsumer.consume(value);
    }

}

package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class FromInt32ToLongListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public FromInt32ToLongListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addInt(int value) {
        listConsumer.consume((long) value);
    }

}

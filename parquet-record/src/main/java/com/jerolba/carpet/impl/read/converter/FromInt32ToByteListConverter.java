package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class FromInt32ToByteListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public FromInt32ToByteListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addInt(int value) {
        listConsumer.consume((byte) value);
    }

}

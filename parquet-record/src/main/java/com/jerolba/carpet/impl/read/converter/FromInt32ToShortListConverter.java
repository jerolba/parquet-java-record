package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class FromInt32ToShortListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public FromInt32ToShortListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addInt(int value) {
        listConsumer.consume((short) value);
    }

}

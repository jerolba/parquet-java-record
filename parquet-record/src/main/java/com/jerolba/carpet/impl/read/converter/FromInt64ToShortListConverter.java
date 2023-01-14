package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class FromInt64ToShortListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public FromInt64ToShortListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addLong(long value) {
        listConsumer.consume((short) value);
    }

}

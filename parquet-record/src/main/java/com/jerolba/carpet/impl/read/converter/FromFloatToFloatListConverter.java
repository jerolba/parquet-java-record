package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class FromFloatToFloatListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public FromFloatToFloatListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addFloat(float value) {
        listConsumer.consume(value);
    }

}

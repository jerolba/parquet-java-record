package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class FromDoubleToDoubleListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public FromDoubleToDoubleListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addDouble(double value) {
        listConsumer.consume(value);
    }

}

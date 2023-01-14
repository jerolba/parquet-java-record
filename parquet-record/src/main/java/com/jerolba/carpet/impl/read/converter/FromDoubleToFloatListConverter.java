package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class FromDoubleToFloatListConverter extends PrimitiveConverter {

    private final ListElementConsumer listConsumer;

    public FromDoubleToFloatListConverter(ListElementConsumer listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addDouble(double value) {
        listConsumer.consume((float) value);
    }

}

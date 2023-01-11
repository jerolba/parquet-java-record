package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ConstructorParams;

public class FromDoubleToDoubleConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public FromDoubleToDoubleConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addDouble(double value) {
        constructor.c[idx] = value;
    }

}

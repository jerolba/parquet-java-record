package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class FromFloatToDoubleConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public FromFloatToDoubleConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addFloat(float value) {
        constructor.c[idx] = (double) value;
    }

}

package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class FromIntToIntegerConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public FromIntToIntegerConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addInt(int value) {
        constructor.c[idx] = value;
    }

    @Override
    public void addLong(long value) {
        constructor.c[idx] = (int) value;
    }

}
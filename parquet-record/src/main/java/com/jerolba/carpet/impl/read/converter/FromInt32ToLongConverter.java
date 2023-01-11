package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ConstructorParams;

public class FromInt32ToLongConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public FromInt32ToLongConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addInt(int value) {
        constructor.c[idx] = (long) value;
    }

}

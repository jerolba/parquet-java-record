package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class FromInt64ToLongConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public FromInt64ToLongConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addLong(long value) {
        constructor.c[idx] = value;
    }

}

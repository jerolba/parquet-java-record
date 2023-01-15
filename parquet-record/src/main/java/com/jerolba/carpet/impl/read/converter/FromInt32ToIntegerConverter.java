package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection;
import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class FromInt32ToIntegerConverter extends PrimitiveConverter {

    private final ReadReflection.ConstructorParams constructor;
    private final int idx;

    public FromInt32ToIntegerConverter(ReadReflection.ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addInt(int value) {
        constructor.c[idx] = value;
    }

}

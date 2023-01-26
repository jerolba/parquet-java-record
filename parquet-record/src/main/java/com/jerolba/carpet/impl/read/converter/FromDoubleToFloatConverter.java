package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class FromDoubleToFloatConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public FromDoubleToFloatConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addDouble(double value) {
        constructor.c[idx] = (float) value;
    }

}

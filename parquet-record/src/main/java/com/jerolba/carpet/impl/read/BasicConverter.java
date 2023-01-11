package com.jerolba.carpet.impl.read;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ConstructorParams;

public class BasicConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public BasicConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addBoolean(boolean value) {
        constructor.c[idx] = value;
    }

    @Override
    public void addDouble(double value) {
        constructor.c[idx] = value;
    }

    @Override
    public void addFloat(float value) {
        constructor.c[idx] = value;
    }

    @Override
    public void addInt(int value) {
        constructor.c[idx] = value;
    }

    @Override
    public void addLong(long value) {
        constructor.c[idx] = value;
    }

}

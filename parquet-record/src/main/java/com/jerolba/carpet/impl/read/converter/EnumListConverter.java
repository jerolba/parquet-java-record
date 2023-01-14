package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;

public class EnumListConverter extends PrimitiveConverter {

    private Enum<?>[] dict = null;
    private final ListElementConsumer listConsumer;
    private final Class<? extends Enum> asEnum;

    public EnumListConverter(ListElementConsumer listConsumer, Class<?> type) {
        this.listConsumer = listConsumer;
        this.asEnum = type.asSubclass(Enum.class);
    }

    @Override
    public void addBinary(Binary value) {
        listConsumer.consume(convert(value));
    }

    @Override
    public boolean hasDictionarySupport() {
        return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        dict = new Enum[dictionary.getMaxId() + 1];
        for (int i = 0; i <= dictionary.getMaxId(); i++) {
            dict[i] = convert(dictionary.decodeToBinary(i));
        }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        listConsumer.consume(dict[dictionaryId]);
    }

    private Enum<?> convert(Binary value) {
        String str = value.toStringUsingUTF8();
        return Enum.valueOf(asEnum, str);
    }
}
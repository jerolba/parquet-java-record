package com.jerolba.carpet.impl.read;

import java.util.List;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedMap;

public class CarpetMapConverter extends GroupConverter {

    private final Consumer<Object> groupConsumer;
    private final Converter converter;
    private final MapHolder mapHolder;

    public CarpetMapConverter(GroupType requestedSchema, ParameterizedMap parameterized,
            Consumer<Object> groupConsumer) {
        this.groupConsumer = groupConsumer;
        this.mapHolder = new MapHolder();
        System.out.println(requestedSchema);

        // Discover converters
        List<Type> fields = requestedSchema.getFields();
        if (fields.size() > 1) {
            throw new RecordTypeConversionException(
                    requestedSchema.getName() + " MAP can not have more than one field");
        }
        Type mapChild = fields.get(0);
        converter = new CarpetMapIntermediateConverter(parameterized, mapChild.asGroupType(), mapHolder);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converter;
    }

    @Override
    public void start() {
        mapHolder.start();
    }

    @Override
    public void end() {
        Object currentRecord = mapHolder.end();
        groupConsumer.accept(currentRecord);
    }

}
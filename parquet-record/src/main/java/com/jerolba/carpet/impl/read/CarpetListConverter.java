package com.jerolba.carpet.impl.read;

import java.util.List;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.AnnotatedLevels;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;

public class CarpetListConverter extends GroupConverter {

    private final Consumer<Object> groupConsumer;
    private final Converter converter;
    private final ListHolder listHolder;
    private final AnnotatedLevels levels;

    public CarpetListConverter(GroupType requestedSchema, ParameterizedCollection parameterized,
            Consumer<Object> groupConsumer) {
        this.groupConsumer = groupConsumer;
        this.listHolder = new ListHolder();
        System.out.println(requestedSchema);

        List<Type> fields = requestedSchema.getFields();
        if (fields.size() > 1) {
            throw new RecordTypeConversionException(
                    requestedSchema.getName() + " LIST can not have more than one field");
        }
        Type listChild = fields.get(0);
        // Implement some logic to see if we have 2 or 3 level structures
        levels = AnnotatedLevels.THREE;
        if (levels == AnnotatedLevels.THREE) {
            converter = new CarpetListIntermediateConverter(listChild.asGroupType(), parameterized, listHolder);
        } else {
            converter = null;
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converter;
    }

    @Override
    public void start() {
        listHolder.start();
    }

    @Override
    public void end() {
        Object currentRecord = listHolder.end();
        groupConsumer.accept(currentRecord);
    }

}
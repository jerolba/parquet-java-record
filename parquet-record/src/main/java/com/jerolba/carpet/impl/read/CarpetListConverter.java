package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.impl.read.PrimitiveGenericConverterFactory.buildPrimitiveGenericConverters;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.impl.ParameterizedCollection;

class CarpetListConverter extends GroupConverter {

    private final Consumer<Object> groupConsumer;
    private final Converter converter;
    private final ListHolder listHolder;

    public CarpetListConverter(GroupType requestedSchema, ParameterizedCollection parameterized,
            Consumer<Object> groupConsumer) {
        this.groupConsumer = groupConsumer;
        this.listHolder = new ListHolder();
        System.out.println(requestedSchema);

        Type listChild = requestedSchema.getFields().get(0);
        boolean threeLevel = SchemaValidation.isThreeLevel(listChild);
        // Implement some logic to see if we have 2 or 3 level structures
        if (threeLevel) {
            converter = new CarpetListIntermediateConverter(listChild, parameterized, listHolder);
        } else {
            converter = createLevel2Converter(listChild, parameterized, listHolder);
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

    private Converter createLevel2Converter(Type listElement, ParameterizedCollection parameterized,
            ListHolder listHolder) {
        if (listElement.isPrimitive()) {
            return buildPrimitiveGenericConverters(listElement, parameterized.getActualType(), listHolder::add);
        }
        LogicalTypeAnnotation logicalType = listElement.getLogicalTypeAnnotation();
        if (logicalType == listType() && parameterized.isCollection()) {
            var parameterizedList = parameterized.getParametizedAsCollection();
            return new CarpetListConverter(listElement.asGroupType(), parameterizedList, listHolder::add);
        }
        if (logicalType == mapType() && parameterized.isMap()) {
            var parameterizedMap = parameterized.getParametizedAsMap();
            return new CarpetMapConverter(listElement.asGroupType(), parameterizedMap, listHolder::add);

        }
        GroupType groupType = listElement.asGroupType();
        Class<?> listType = parameterized.getActualType();
        return new CarpetGroupConverter(groupType, listType, listHolder::add);
    }

}
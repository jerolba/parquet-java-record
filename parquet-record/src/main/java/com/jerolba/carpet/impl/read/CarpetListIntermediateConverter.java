package com.jerolba.carpet.impl.read;

import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.util.List;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;

class CarpetListIntermediateConverter extends GroupConverter {

    private final Converter converter;
    private final ListHolder listHolder;
    private Object elementValue;

    public CarpetListIntermediateConverter(GroupType requestedSchema, ParameterizedCollection parameterized,
            ListHolder listHolder) {
        System.out.println(requestedSchema);
        this.listHolder = listHolder;

        List<Type> fields = requestedSchema.getFields();
        if (fields.size() > 1) {
            throw new RecordTypeConversionException(
                    requestedSchema.getName() + " LIST child element can not have more than one field");
        }
        Type listElement = fields.get(0);
        if (listElement.isPrimitive()) {
            converter = PrimitiveGenericConverterFactory.buildPrimitiveGenericConverters(listElement, parameterized.getActualType(), this::accept);
            return;
        }
        LogicalTypeAnnotation logicalType = listElement.getLogicalTypeAnnotation();
        if (logicalType == listType() && parameterized.isCollection()) {
            var parameterizedList = parameterized.getParametizedAsCollection();
            converter = new CarpetListConverter(listElement.asGroupType(), parameterizedList, this::accept);
            return;
        }
        if (logicalType == mapType() && parameterized.isMap()) {
            var parameterizedMap = parameterized.getParametizedAsMap();
            converter = new CarpetMapConverter(listElement.asGroupType(), parameterizedMap, this::accept);
            return;

        }
        GroupType groupType = listElement.asGroupType();
        Class<?> listType = parameterized.getActualType();
        converter = new CarpetGroupConverter(groupType, listType, this::accept);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converter;
    }

    @Override
    public void start() {
        elementValue = null;
    }

    @Override
    public void end() {
        listHolder.add(elementValue);
    }

    public void accept(Object value) {
        elementValue = value;
    }

}
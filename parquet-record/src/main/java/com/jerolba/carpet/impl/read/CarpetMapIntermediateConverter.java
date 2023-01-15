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
import com.jerolba.carpet.impl.ParameterizedMap;

public class CarpetMapIntermediateConverter extends GroupConverter {

    private final Converter converterValue;
    private final Converter converterKey;
    private final MapHolder mapHolder;
    private Object elementValue;
    private Object elementKey;

    public CarpetMapIntermediateConverter(ParameterizedMap parameterized, GroupType requestedSchema,
            MapHolder mapHolder) {
        System.out.println(requestedSchema);
        this.mapHolder = mapHolder;

        List<Type> fields = requestedSchema.getFields();
        if (fields.size() != 2) {
            throw new RecordTypeConversionException(
                    requestedSchema.getName() + " MAP child element must have two fields");
        }

        // Key
        Type mapKeyType = fields.get(0);
        if (mapKeyType.isPrimitive()) {
            converterKey = PrimitiveGenericConverterFactory.buildPrimitiveGenericConverters(mapKeyType, parameterized.getKeyActualType(),
                    this::consumeKey);
        } else {
            GroupType mapKeyGroupType = mapKeyType.asGroupType();
            Class<?> mapKeyActualType = parameterized.getKeyActualType();
            converterKey = new CarpetGroupConverter(mapKeyGroupType, mapKeyActualType, this::consumeKey);
        }

        // Value
        Type mapValueType = fields.get(1);
        if (mapValueType.isPrimitive()) {
            converterValue = PrimitiveGenericConverterFactory.buildPrimitiveGenericConverters(mapValueType,
                    parameterized.getValueActualType(),
                    this::consumeValue);
            return;
        }
        LogicalTypeAnnotation logicalType = mapValueType.getLogicalTypeAnnotation();
        if (logicalType == listType() && parameterized.valueIsCollection()) {
            var parameterizedValue = parameterized.getValueTypeAsCollection();
            converterValue = new CarpetListConverter(mapValueType.asGroupType(), parameterizedValue,
                    this::consumeValue);
            return;
        }
        if (logicalType == mapType() && parameterized.valueIsMap()) {
            var parameterizedValue = parameterized.getValueTypeAsMap();
            converterValue = new CarpetMapConverter(mapValueType.asGroupType(), parameterizedValue,
                    this::consumeValue);
            return;
        }
        GroupType mapValueGroupType = mapValueType.asGroupType();
        Class<?> mapValueActualType = parameterized.getValueActualType();
        converterValue = new CarpetGroupConverter(mapValueGroupType, mapValueActualType, this::consumeValue);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        if (fieldIndex == 0) {
            return converterKey;
        }
        return converterValue;
    }

    @Override
    public void start() {
        elementKey = null;
        elementValue = null;
    }

    @Override
    public void end() {
        mapHolder.put(elementKey, elementValue);
    }

    public void consumeKey(Object value) {
        elementKey = value;
    }

    public void consumeValue(Object value) {
        elementValue = value;
    }

}
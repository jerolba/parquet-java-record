package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedMap;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildPrimitiveConverters;
import static com.jerolba.carpet.impl.read.SingleLevelConverterFactory.createSingleLevelConverter;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.RecordTypeConversionException;

public class CarpetGroupConverter extends GroupConverter {

    private final Converter[] converters;
    private final ReadReflection.ConstructorParams constructor;
    private final Consumer<Object> groupConsumer;

    public CarpetGroupConverter(GroupType requestedSchema, Class<?> groupClass, Consumer<Object> groupConsumer) {
        this.groupConsumer = groupConsumer;
        this.constructor = new ReadReflection.ConstructorParams(groupClass);
        System.out.println(requestedSchema);

        GroupFieldsMapper mapper = new GroupFieldsMapper(groupClass);

        converters = new Converter[requestedSchema.getFields().size()];
        int cont = 0;
        for (var schemaField : requestedSchema.getFields()) {
            String name = schemaField.getName();
            int index = mapper.getIndex(name);
            var recordComponent = mapper.getRecordComponent(name);
            if (recordComponent == null) {
                throw new RecordTypeConversionException(
                        groupClass.getName() + " doesn't have an attribute called " + name);
            }
            if (schemaField.isRepetition(Repetition.REPEATED)) {
                converters[cont] = createSingleLevelConverter(schemaField, constructor, index, recordComponent);
            } else if (schemaField.isPrimitive()) {
                converters[cont] = buildPrimitiveConverters(schemaField, constructor, index, recordComponent);
            } else {
                GroupType asGroupType = schemaField.asGroupType();
                LogicalTypeAnnotation logicalType = asGroupType.getLogicalTypeAnnotation();
                if (logicalType == listType()) {
                    var parameterized = getParameterizedCollection(recordComponent);
                    converters[cont] = new CarpetListConverter(asGroupType, parameterized,
                            value -> constructor.c[index] = value);
                } else if (logicalType == mapType()) {
                    var parameterized = getParameterizedMap(recordComponent);
                    converters[cont] = new CarpetMapConverter(asGroupType, parameterized,
                            value -> constructor.c[index] = value);
                } else {
                    Class<?> childClass = recordComponent.getType();
                    CarpetGroupConverter converter = new CarpetGroupConverter(asGroupType, childClass,
                            value -> constructor.c[index] = value);
                    converters[cont] = converter;
                }
            }
            cont++;
        }
    }

    public Object getCurrentRecord() {
        return constructor.create();
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
        Arrays.fill(constructor.c, null);
    }

    @Override
    public void end() {
        Object currentRecord = getCurrentRecord();
        groupConsumer.accept(currentRecord);
    }

}
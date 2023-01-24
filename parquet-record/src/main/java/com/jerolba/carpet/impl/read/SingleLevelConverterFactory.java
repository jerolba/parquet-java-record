package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.read.PrimitiveGenericConverterFactory.buildPrimitiveGenericConverters;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

/**
 *
 * Supports reading 1-level collection structure. The field has repeated
 * repetition, but is not declared with a list LogicalType.
 *
 * Examples:
 *
 * Repeated list integer: List<Integer>
 *
 * <pre>
 * repeated int32 sizes
 * </pre>
 *
 * Repeated list of status record: List<Status>
 *
 * <pre>
 * repeated group status {
 *   optional binary id (STRING);
 *   required boolean active;
 * }
 * </pre>
 *
 */
class SingleLevelConverterFactory {

    public static Converter createSingleLevelConverter(Type parquetField, ConstructorParams constructor,
            int index, RecordComponent recordComponent) {

        Consumer<Object> consumer = v -> {
            if (constructor.c[index] == null) {
                constructor.c[index] = new ArrayList<>();
            }
            ((List) constructor.c[index]).add(v);
        };

        var parameterized = getParameterizedCollection(recordComponent);
        if (parquetField.isPrimitive()) {
            return buildPrimitiveGenericConverters(parquetField, parameterized.getActualType(), consumer);
        }
        var asGroupType = parquetField.asGroupType();
        if (parameterized.isMap()) {
            return new CarpetMapConverter(asGroupType, parameterized.getParametizedAsMap(), consumer);
        }
        var actualCollectionType = parameterized.getActualType();
        if (actualCollectionType.isRecord()) {
            return new CarpetGroupConverter(asGroupType, actualCollectionType, consumer);
        }
        throw new RecordTypeConversionException("Unexpected single level collection schema");
    }

}

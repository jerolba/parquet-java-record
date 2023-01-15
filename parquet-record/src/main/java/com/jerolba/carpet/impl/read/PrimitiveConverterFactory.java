package com.jerolba.carpet.impl.read;

import java.lang.reflect.RecordComponent;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.converter.BooleanConverter;
import com.jerolba.carpet.impl.read.converter.EnumConverter;
import com.jerolba.carpet.impl.read.converter.FromDoubleToDoubleConverter;
import com.jerolba.carpet.impl.read.converter.FromDoubleToFloatConverter;
import com.jerolba.carpet.impl.read.converter.FromFloatToDoubleConverter;
import com.jerolba.carpet.impl.read.converter.FromFloatToFloatConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToByteConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToIntegerConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToLongConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToShortConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToByteConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToIntegerConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToLongConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToShortConverter;
import com.jerolba.carpet.impl.read.converter.StringConverter;

class PrimitiveConverterFactory {

    public static Converter buildPrimitiveConverters(Type parquetField, ReadReflection.ConstructorParams constructor,
            int index,
            RecordComponent recordComponent) {

        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        switch (type) {
        case INT32:
            return buildFromInt32(constructor, index, recordComponent);
        case INT64:
            return buildFromInt64Converter(constructor, index, recordComponent);
        case FLOAT:
            return buildFromFloatConverter(constructor, index, recordComponent);
        case DOUBLE:
            return buildFromDoubleConverter(constructor, index, recordComponent);
        case BOOLEAN:
            return buildFromBooleanConverter(constructor, index, recordComponent);
        case BINARY:
            return buildFromBinaryConverter(constructor, index, recordComponent, parquetField);
        case INT96, FIXED_LEN_BYTE_ARRAY:
            throw new RecordTypeConversionException(type + " deserialization not supported");
        }
        throw new RecordTypeConversionException(type + " deserialization not supported");
    }

    public static Converter buildFromInt64Converter(ReadReflection.ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new FromInt64ToIntegerConverter(constructor, index);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new FromInt64ToLongConverter(constructor, index);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return new FromInt64ToShortConverter(constructor, index);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new FromInt64ToByteConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromInt32(ReadReflection.ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new FromInt32ToIntegerConverter(constructor, index);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new FromInt32ToLongConverter(constructor, index);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return new FromInt32ToShortConverter(constructor, index);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new FromInt32ToByteConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromDoubleConverter(ReadReflection.ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new FromDoubleToFloatConverter(constructor, index);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new FromDoubleToDoubleConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromFloatConverter(ReadReflection.ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new FromFloatToFloatConverter(constructor, index);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new FromFloatToDoubleConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromBooleanConverter(ReadReflection.ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return new BooleanConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromBinaryConverter(ReadReflection.ConstructorParams constructor, int index,
            RecordComponent recordComponent, Type schemaType) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        if (logicalType.equals(LogicalTypeAnnotation.stringType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringConverter(constructor, index);
            }
            throw new RecordTypeConversionException(typeName + " not compatible with String field");
        }
        if (logicalType.equals(LogicalTypeAnnotation.enumType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringConverter(constructor, index);
            }
            return new EnumConverter(constructor, index, recordComponent.getType());

        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

}

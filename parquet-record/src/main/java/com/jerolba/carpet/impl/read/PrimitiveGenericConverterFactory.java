package com.jerolba.carpet.impl.read;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.converter.BooleanGenericConverter;
import com.jerolba.carpet.impl.read.converter.EnumGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromDoubleToDoubleGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromDoubleToFloatGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromFloatToDoubleGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromFloatToFloatGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToByteGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToIntegerGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToLongGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToShortGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToByteGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToIntegerGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToLongGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToShortGenericConverter;
import com.jerolba.carpet.impl.read.converter.StringGenericConverter;

public class PrimitiveGenericConverterFactory {

    public static Converter genericBuildFromInt64Converter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new FromInt64ToIntegerGenericConverter(listConsumer);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new FromInt64ToLongGenericConverter(listConsumer);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return new FromInt64ToShortGenericConverter(listConsumer);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new FromInt64ToByteGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromInt32(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new FromInt32ToIntegerGenericConverter(listConsumer);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new FromInt32ToLongGenericConverter(listConsumer);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return new FromInt32ToShortGenericConverter(listConsumer);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new FromInt32ToByteGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromDoubleConverter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new FromDoubleToFloatGenericConverter(listConsumer);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new FromDoubleToDoubleGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromFloatConverter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new FromFloatToFloatGenericConverter(listConsumer);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new FromFloatToDoubleGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromBooleanConverter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return new BooleanGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromBinaryConverter(Consumer<Object> listConsumer, Class<?> type,
            Type schemaType) {
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        String typeName = type.getName();
        if (logicalType.equals(LogicalTypeAnnotation.stringType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringGenericConverter(listConsumer);
            }
            throw new RecordTypeConversionException(typeName + " not compatible with String field");
        }
        if (logicalType.equals(LogicalTypeAnnotation.enumType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringGenericConverter(listConsumer);
            }
            return new EnumGenericConverter(listConsumer, type);

        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + schemaType.getName() + " field");
    }

}

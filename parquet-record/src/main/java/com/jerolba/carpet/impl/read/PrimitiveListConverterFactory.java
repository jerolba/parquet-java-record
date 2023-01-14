package com.jerolba.carpet.impl.read;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.CarpetReader.ListElementConsumer;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.converter.BooleanListConverter;
import com.jerolba.carpet.impl.read.converter.FromDoubleToDoubleListConverter;
import com.jerolba.carpet.impl.read.converter.FromDoubleToFloatListConverter;
import com.jerolba.carpet.impl.read.converter.FromFloatToDoubleListConverter;
import com.jerolba.carpet.impl.read.converter.FromFloatToFloatListConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToByteListConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToIntegerListConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToLongListConverter;
import com.jerolba.carpet.impl.read.converter.FromInt32ToShortListConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToByteListConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToIntegerListConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToLongListConverter;
import com.jerolba.carpet.impl.read.converter.FromInt64ToShortListConverter;
import com.jerolba.carpet.impl.read.converter.StringListConverter;

public class PrimitiveListConverterFactory {

    public static Converter listBuildFromInt64Converter(ListElementConsumer listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new FromInt64ToIntegerListConverter(listConsumer);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new FromInt64ToLongListConverter(listConsumer);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return new FromInt64ToShortListConverter(listConsumer);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new FromInt64ToByteListConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter listBuildFromInt32(ListElementConsumer listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new FromInt32ToIntegerListConverter(listConsumer);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new FromInt32ToLongListConverter(listConsumer);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return new FromInt32ToShortListConverter(listConsumer);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new FromInt32ToByteListConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter listBuildFromDoubleConverter(ListElementConsumer listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new FromDoubleToFloatListConverter(listConsumer);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new FromDoubleToDoubleListConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter listBuildFromFloatConverter(ListElementConsumer listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new FromFloatToFloatListConverter(listConsumer);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new FromFloatToDoubleListConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter listBuildFromBooleanConverter(ListElementConsumer listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return new BooleanListConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter listBuildFromBinaryConverter(ListElementConsumer listConsumer, Class<?> type,
            Type schemaType) {
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        if (logicalType.equals(LogicalTypeAnnotation.stringType())) {
            String typeName = type.getName();
            if (typeName.equals("java.lang.String")) {
                return new StringListConverter(listConsumer);
            }
        }
        throw new RecordTypeConversionException(
                type.getName() + " not compatible with " + schemaType.getName() + " field");
    }

}

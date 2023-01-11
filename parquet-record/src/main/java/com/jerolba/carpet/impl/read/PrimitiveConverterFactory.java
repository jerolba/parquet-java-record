package com.jerolba.carpet.impl.read;

import java.lang.reflect.RecordComponent;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.CarpetReader.ConstructorParams;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.converter.BooleanConverter;
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
import com.jerolba.carpet.impl.read.converter.StringConverter;

public class PrimitiveConverterFactory {

    public static Converter buildFromInt64Converter(ConstructorParams constructor, int index,
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
            return new FromInt32ToShortConverter(constructor, index);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new FromInt64ToByteConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromInt32(ConstructorParams constructor, int index, RecordComponent recordComponent) {
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

    public static Converter buildFromDoubleConverter(ConstructorParams constructor, int index,
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

    public static Converter buildFromFloatConverter(ConstructorParams constructor, int index,
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

    public static Converter buildFromBooleanConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return new BooleanConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromBinaryConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent, Type schemaType) {
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        if (logicalType.equals(LogicalTypeAnnotation.stringType())) {
            Class<?> type = recordComponent.getType();
            String typeName = type.getName();
            if (typeName.equals("java.lang.String")) {
                return new StringConverter(constructor, index);
            }
        }
        throw new RecordTypeConversionException(
                recordComponent.getType().getName() + " not compatible with " + recordComponent.getName()
                        + " field");
    }

}

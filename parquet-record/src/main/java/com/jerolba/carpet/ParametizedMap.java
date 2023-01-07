package com.jerolba.carpet;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.Map;

class ParametizedMap {

    private final Type keyType;
    private final Type valueType;

    public ParametizedMap(ParameterizedType type) {
        this.keyType = type.getActualTypeArguments()[0];
        this.valueType = type.getActualTypeArguments()[1];
    }

    public static ParametizedMap getMapClass(RecordComponent attr) {
        java.lang.reflect.Type genericType = attr.getGenericType();
        if (genericType instanceof TypeVariable<?>) {
            throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
        }
        if (genericType instanceof ParameterizedType paramType) {
            return new ParametizedMap(paramType);
        }
        throw new RecordTypeConversionException("Unsuported type in map");
    }

    public Class<?> getValueActualType() {
        if ((valueType instanceof Class<?> finalType)) {
            return finalType;
        }
        if ((valueType instanceof TypeVariable<?> finalType)) {
            throw new RecordTypeConversionException(
                    finalType.getName() + " generic type not supported as value of a Map");
        }
        throw new RecordTypeConversionException("Invalid type in value Map " + valueType);
    }

    public Class<?> getKeyActualType() {
        if ((keyType instanceof Class<?> finalType)) {
            return finalType;
        }
        if ((keyType instanceof TypeVariable<?> finalType)) {
            throw new RecordTypeConversionException(
                    finalType.getName() + " generic type not supported as key of a Map");
        }
        throw new RecordTypeConversionException("Invalid type in key Map " + keyType);
    }

    public ParametizedMap getValueTypeAsMap() {
        if (valueType instanceof ParameterizedType paramType) {
            return new ParametizedMap(paramType);
        }
        return null;
    }

    public ParametizedObject getValueTypeAsCollection() {
        if (valueType instanceof ParameterizedType paramType) {
            return new ParametizedObject(paramType);
        }
        return null;
    }

    public boolean valueIsCollection() {
        if (valueType instanceof ParameterizedType paramType) {
            Type collectionActualType = paramType.getRawType();
            if ((collectionActualType instanceof Class<?> finalType)) {
                if (Collection.class.isAssignableFrom(finalType)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return false;
    }

    public boolean valueIsMap() {
        if (valueType instanceof ParameterizedType paramType) {
            Type collectionActualType = paramType.getRawType();
            if ((collectionActualType instanceof Class<?> finalType)) {
                if (Map.class.isAssignableFrom(finalType)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return false;
    }
}
package com.jerolba.carpet;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;

class ParametizedObject {

    private final Type collectionType;

    public ParametizedObject(ParameterizedType type) {
        this.collectionType = type.getActualTypeArguments()[0];
    }

    public static ParametizedObject getCollectionClass(RecordComponent attr) {
        java.lang.reflect.Type genericType = attr.getGenericType();
        if (genericType instanceof TypeVariable<?>) {
            throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
        }
        if (genericType instanceof ParameterizedType paramType) {
            return new ParametizedObject(paramType);
        }
        throw new RecordTypeConversionException("Unsuported type in collection");
    }

    public Class<?> getActualType() {
        if ((collectionType instanceof Class<?> finalType)) {
            return finalType;
        }
        if ((collectionType instanceof TypeVariable<?> finalType)) {
            throw new RecordTypeConversionException(finalType.getName() + " generic type not supported");
        }
        throw new RecordTypeConversionException("Invalid type in collection " + collectionType);
    }

    public boolean isCollection() {
        if (collectionType instanceof ParameterizedType paramType) {
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

    public boolean isMap() {
        return false;
    }

    public ParametizedObject getParametizedChild() {
        if (collectionType instanceof ParameterizedType paramType) {
            return new ParametizedObject(paramType);
        }
        return null;
    }

}
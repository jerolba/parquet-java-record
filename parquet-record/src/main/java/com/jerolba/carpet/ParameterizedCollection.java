package com.jerolba.carpet;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

class ParameterizedCollection {

    private final Type collectionType;

    public ParameterizedCollection(ParameterizedType type) {
        this.collectionType = type.getActualTypeArguments()[0];
    }

    public Class<?> getActualType() {
        return Parametized.getClassFromType(collectionType, "in Collection");
    }

    public ParameterizedCollection getParametizedAsCollection() {
        if (collectionType instanceof ParameterizedType paramType) {
            return new ParameterizedCollection(paramType);
        }
        return null;
    }

    public ParameterizedMap getParametizedAsMap() {
        if (collectionType instanceof ParameterizedType paramType) {
            return new ParameterizedMap(paramType);
        }
        return null;
    }

    public boolean isCollection() {
        return Parametized.isCollection(collectionType);
    }

    public boolean isMap() {
        return Parametized.isMap(collectionType);
    }

}
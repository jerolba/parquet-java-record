package com.jerolba.carpet.impl;

import static com.jerolba.carpet.impl.AliasField.getFieldName;
import static com.jerolba.carpet.impl.NotNullField.isNotNull;
import static com.jerolba.carpet.impl.Parametized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parametized.getParameterizedMap;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import java.lang.reflect.RecordComponent;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.CarpetConfiguration;
import com.jerolba.carpet.RecordTypeConversionException;

import org.apache.parquet.schema.Types;

public class JavaRecord2Schema {

    private final CarpetConfiguration carpetConfiguration;

    public JavaRecord2Schema(CarpetConfiguration carpetConfiguration) {
        this.carpetConfiguration = carpetConfiguration;
    }

    public MessageType createSchema(Class<?> recordClass) {
        return build(recordClass, new HashSet<>());
    }

    private MessageType build(Class<?> recordClass, Set<Class<?>> visited) {
        validateNotVisitedRecord(recordClass, visited);
        String groupName = recordClass.getSimpleName();

        List<Type> fields = createGroupFields(recordClass, visited);
        return new MessageType(groupName, fields);
    }

    private List<Type> createGroupFields(Class<?> recordClass, Set<Class<?>> visited) {
        List<Type> fields = new ArrayList<>();
        for (var attr : recordClass.getRecordComponents()) {
            Class<?> type = attr.getType();
            String fieldName = getFieldName(attr);
            boolean notNull = type.isPrimitive() || isNotNull(attr);
            Repetition repetition = notNull ? REQUIRED : OPTIONAL;

            PrimitiveTypeName primitiveType = simpleTypeItems(type);
            if (primitiveType != null) {
                fields.add(new PrimitiveType(repetition, primitiveType, fieldName));
            } else if (type.getName().equals("java.lang.String")) {
                fields.add(Types.primitive(BINARY, repetition).as(stringType()).named(fieldName));
            } else if (type.isRecord()) {
                List<Type> childFields = buildCompositeChild(type, visited);
                fields.add(new GroupType(repetition, fieldName, childFields));
            } else if (type.isEnum()) {
                fields.add(Types.primitive(BINARY, repetition).as(enumType()).named(fieldName));
            } else if (Collection.class.isAssignableFrom(type)) {
                var parameterizedCollection = getParameterizedCollection(attr);
                fields.add(createCollectionType(fieldName, parameterizedCollection, visited, attr, repetition));
            } else if (Map.class.isAssignableFrom(type)) {
                var parameterizedMap = getParameterizedMap(attr);
                fields.add(createMapType(fieldName, parameterizedMap, visited, repetition));
            } else {
                java.lang.reflect.Type genericType = attr.getGenericType();
                if (genericType instanceof TypeVariable<?>) {
                    throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
                }
            }
        }
        return fields;
    }

    private List<Type> buildCompositeChild(Class<?> recordClass, Set<Class<?>> visited) {
        validateNotVisitedRecord(recordClass, visited);
        List<Type> fields = createGroupFields(recordClass, visited);
        visited.remove(recordClass);
        return fields;
    }

    private Type createCollectionType(String fieldName, ParameterizedCollection collectionClass, Set<Class<?>> visited,
            RecordComponent attr, Repetition repetition) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> createCollectionOneLevel(fieldName, collectionClass, visited);
        case TWO -> createCollectionTwoLevel(fieldName, collectionClass, visited, repetition);
        case THREE -> createCollectionThreeLevel(fieldName, collectionClass, visited, repetition);
        };
    }

    private Type createCollectionOneLevel(String fieldName, ParameterizedCollection parametized,
            Set<Class<?>> visited) {
        if (parametized.isCollection()) {
            throw new RecordTypeConversionException(
                    "Recursive collections not supported in annotated 1-level structures");
        }
        if (parametized.isMap()) {
            return createMapType(fieldName, parametized.getParametizedAsMap(), visited, REPEATED);
        }
        Class<?> type = parametized.getActualType();
        return buildTypeElement(type, visited, REPEATED, fieldName);
    }

    private Type createCollectionTwoLevel(String fieldName, ParameterizedCollection parametized, Set<Class<?>> visited,
            Repetition repetition) {
        Type nested = createCollectionNestedTwoLevel(parametized, visited);
        return ConversionPatterns.listType(repetition, fieldName, nested);
    }

    private Type createCollectionNestedTwoLevel(ParameterizedCollection parametized, Set<Class<?>> visited) {
        if (parametized.isCollection()) {
            return createCollectionType("element", parametized.getParametizedAsCollection(), visited, null, REPEATED);
        }
        if (parametized.isMap()) {
            return createMapType("element", parametized.getParametizedAsMap(), visited, REPEATED);
        }
        Class<?> type = parametized.getActualType();
        return buildTypeElement(type, visited, REPEATED, "element");
    }

    private Type createCollectionThreeLevel(String fieldName, ParameterizedCollection parametized,
            Set<Class<?>> visited, Repetition repetition) {
        Type nested = createCollectionNestedThreeLevel(parametized, visited);
        return ConversionPatterns.listOfElements(repetition, fieldName, nested);
    }

    private Type createCollectionNestedThreeLevel(ParameterizedCollection parametized, Set<Class<?>> visited) {
        if (parametized.isCollection()) {
            return createCollectionType("element", parametized.getParametizedAsCollection(), visited, null, OPTIONAL);
        }
        if (parametized.isMap()) {
            return createMapType("element", parametized.getParametizedAsMap(), visited, OPTIONAL);
        }
        Class<?> type = parametized.getActualType();
        return buildTypeElement(type, visited, OPTIONAL, "element");
    }

    private Type createMapType(String fieldName, ParameterizedMap parametized, Set<Class<?>> visited,
            Repetition repetition) {
        Class<?> keyType = parametized.getKeyActualType();
        Type nestedKey = buildTypeElement(keyType, visited, REQUIRED, "key");

        if (parametized.valueIsCollection()) {
            Type childCollection = createCollectionType("value", parametized.getValueTypeAsCollection(),
                    visited, null, OPTIONAL);
            return Types.map(repetition).key(nestedKey).value(childCollection).named(fieldName);
        }
        if (parametized.valueIsMap()) {
            Type childMap = createMapType("value", parametized.getValueTypeAsMap(), visited, OPTIONAL);
            return Types.map(repetition).key(nestedKey).value(childMap).named(fieldName);
        }

        Class<?> valueType = parametized.getValueActualType();
        Type nestedValue = buildTypeElement(valueType, visited, OPTIONAL, "value");
        if (nestedKey != null && nestedValue != null) {
            // TODO: what to change to support generation of older versions?
            return Types.map(repetition).key(nestedKey).value(nestedValue).named(fieldName);
            // return ConversionPatterns.mapType(repetition, fieldName, "key_value",
            // nestedKey, nestedValue);
        }
        throw new RecordTypeConversionException("Unsuported type in Map");
    }

    private Type buildTypeElement(Class<?> type, Set<Class<?>> visited, Repetition repetition, String name) {
        PrimitiveTypeName primitiveKeyType = simpleTypeItems(type);
        if (primitiveKeyType != null) {
            return new PrimitiveType(repetition, primitiveKeyType, name);
        } else if (type.getName().equals("java.lang.String")) {
            return Types.primitive(BINARY, repetition).as(stringType()).named(name);
        } else if (type.isRecord()) {
            List<Type> childFields = buildCompositeChild(type, visited);
            return new GroupType(repetition, name, childFields);
        } else if (type.isEnum()) {
            return Types.primitive(BINARY, repetition).as(enumType()).named(name);
        } else {
            // Generic types in first child are detected
        }
        throw new RecordTypeConversionException("Unsuported type " + type);
    }

    private PrimitiveTypeName simpleTypeItems(Class<?> type) {
        return switch (type.getName()) {
        case "short", "java.lang.Short", "int", "java.lang.Integer" -> PrimitiveTypeName.INT32;
        case "byte", "java.lang.Byte" -> PrimitiveTypeName.INT32;
        case "long", "java.lang.Long" -> PrimitiveTypeName.INT64;
        case "float", "java.lang.Float" -> PrimitiveTypeName.FLOAT;
        case "double", "java.lang.Double" -> PrimitiveTypeName.DOUBLE;
        case "boolean", "java.lang.Boolean" -> PrimitiveTypeName.BOOLEAN;
        default -> null;
        };
    }

    private void validateNotVisitedRecord(Class<?> recordClass, Set<Class<?>> visited) {
        if (!recordClass.isRecord()) {
            throw new RecordTypeConversionException(recordClass.getName() + " must be a java Record");
        }
        if (visited.contains(recordClass)) {
            throw new RecordTypeConversionException("Recusive records are not supported");
        }
        visited.add(recordClass);
    }

}

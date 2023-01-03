package com.jerolba.carpet;

import static com.jerolba.carpet.AliasField.getFieldName;
import static com.jerolba.carpet.NotNullField.isNotNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

public class JavaRecord2Schema {

    private final CarpetConfiguration carpetConfiguration;

    public JavaRecord2Schema(CarpetConfiguration carpetConfiguration) {
        this.carpetConfiguration = carpetConfiguration;
    }

    public MessageType createSchema(Class<?> recordClass) {
        Set<Class<?>> visited = new HashSet<>();
        return build(recordClass, visited);
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
                ParametizedObject collectionClass = getCollectionClass(attr);
                fields.add(createCollectionType(fieldName, collectionClass, visited, attr, repetition));
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

    private Type createCollectionType(String fieldName, ParametizedObject collectionClass, Set<Class<?>> visited,
            RecordComponent attr, Repetition repetition) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> createCollectionOneLevel(fieldName, collectionClass, visited, attr);
        case TWO -> createCollectionTwoLevel(fieldName, collectionClass, visited, repetition);
        case THREE -> createCollectionThreeLevel(fieldName, collectionClass, visited, repetition);
        };
    }

    private Type createCollectionOneLevel(String fieldName, ParametizedObject parametized, Set<Class<?>> visited,
            RecordComponent attr) {
        Repetition repetition = Repetition.REPEATED;

        if (parametized.isCollection() || parametized.isMap()) {
            throw new RecordTypeConversionException(
                    "Recursive collections not supported in annotated 1-level structures");
        }
        Class<?> type = parametized.getActualType();
        PrimitiveTypeName primitiveType = simpleTypeItems(type);
        if (primitiveType != null) {
            return new PrimitiveType(repetition, primitiveType, fieldName);
        } else if (type.getName().equals("java.lang.String")) {
            return Types.primitive(BINARY, repetition).as(stringType()).named(fieldName);
        } else if (type.isRecord()) {
            List<Type> childFields = buildCompositeChild(type, visited);
            return new GroupType(repetition, fieldName, childFields);
        } else if (type.isEnum()) {
            return Types.primitive(BINARY, repetition).as(enumType()).named(fieldName);
        } else {
            // Generic types in first child are detected
        }
        throw new RecordTypeConversionException("Unsuported type in collection");
    }

    private Type createCollectionTwoLevel(String fieldName, ParametizedObject parametized, Set<Class<?>> visited,
            Repetition repetition) {

        if (parametized.isCollection() || parametized.isMap()) {
            Type nested = createCollectionType("element", parametized.getParametizedChild(), visited, null,
                    Repetition.REPEATED);
            return ConversionPatterns.listType(repetition, fieldName, nested);
        }
        Class<?> type = parametized.getActualType();
        PrimitiveTypeName primitiveType = simpleTypeItems(type);
        if (primitiveType != null) {
            var nested = new PrimitiveType(Repetition.REPEATED, primitiveType, "element");
            return ConversionPatterns.listType(repetition, fieldName, nested);
        } else if (type.getName().equals("java.lang.String")) {
            var nested = Types.primitive(BINARY, Repetition.REPEATED).as(stringType()).named("element");
            return ConversionPatterns.listType(repetition, fieldName, nested);
        } else if (type.isRecord()) {
            List<Type> childFields = buildCompositeChild(type, visited);
            var nested = new GroupType(Repetition.REPEATED, "element", childFields);
            return ConversionPatterns.listType(repetition, fieldName, nested);
        } else if (type.isEnum()) {
            var nested = Types.primitive(BINARY, Repetition.REPEATED).as(enumType()).named("element");
            return ConversionPatterns.listType(repetition, fieldName, nested);
        } else {
            // Generic types in first child are detected
        }
        throw new RecordTypeConversionException("Unsuported type in collection");
    }

    private Type createCollectionThreeLevel(String fieldName, ParametizedObject parametized, Set<Class<?>> visited,
            Repetition repetition) {

        if (parametized.isCollection() || parametized.isMap()) {
            Type nested = createCollectionType("element", parametized.getParametizedChild(), visited, null,
                    Repetition.OPTIONAL);
            return ConversionPatterns.listOfElements(repetition, fieldName, nested);
        }
        Class<?> type = parametized.getActualType();
        PrimitiveTypeName primitiveType = simpleTypeItems(type);
        if (primitiveType != null) {
            var nested = new PrimitiveType(Repetition.OPTIONAL, primitiveType, "element");
            return ConversionPatterns.listOfElements(repetition, fieldName, nested);
        } else if (type.getName().equals("java.lang.String")) {
            var nested = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("element");
            return ConversionPatterns.listOfElements(repetition, fieldName, nested);
        } else if (type.isRecord()) {
            List<Type> childFields = buildCompositeChild(type, visited);
            var nested = new GroupType(Repetition.OPTIONAL, "element", childFields);
            return ConversionPatterns.listOfElements(repetition, fieldName, nested);
        } else if (type.isEnum()) {
            var nested = Types.primitive(BINARY, Repetition.OPTIONAL).as(enumType()).named("element");
            return ConversionPatterns.listOfElements(repetition, fieldName, nested);
        } else {
            // Generic types in first child are detected
        }
        throw new RecordTypeConversionException("Unsuported type in collection");
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

    private ParametizedObject getCollectionClass(RecordComponent attr) {
        java.lang.reflect.Type genericType = attr.getGenericType();
        if (genericType instanceof TypeVariable<?>) {
            throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
        }
        if (genericType instanceof ParameterizedType paramType) {
            return new ParametizedObject(paramType);
        }
        throw new RecordTypeConversionException("Unsuported type in collection ");
    }

    private static class ParametizedObject {

        private final java.lang.reflect.Type collectionType;

        public ParametizedObject(ParameterizedType type) {
            this.collectionType = type.getActualTypeArguments()[0];
        }

        public Class<?> getActualType() {
            if ((collectionType instanceof Class<?> finalType)) {
                return finalType;
            }
            throw new RecordTypeConversionException("Invalid type in collection " + collectionType);
        }

        public boolean isCollection() {
            if (collectionType instanceof ParameterizedType paramType) {
                java.lang.reflect.Type collectionActualType = paramType.getRawType();
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

        public ParametizedObject getParametizedChild() {
            if (collectionType instanceof ParameterizedType paramType) {
                return new ParametizedObject(paramType);
            }
            return null;
        }

        public boolean isMap() {
            return false;
        }
    }
}

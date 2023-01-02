package com.jerolba.carpet;

import static com.jerolba.carpet.AliasField.getFieldName;
import static com.jerolba.carpet.NotNullField.isNotNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import java.lang.reflect.RecordComponent;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

public class JavaRecord2Schema {

    public JavaRecord2Schema() {
        // Inject configuration
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
                List<Type> childFields = buildComposisteChild(attr, type, visited);
                fields.add(new GroupType(repetition, fieldName, childFields));
            } else if (type.isEnum()) {
                fields.add(Types.primitive(BINARY, repetition).as(enumType()).named(fieldName));
            } else {
                java.lang.reflect.Type genericType = attr.getGenericType();
                if (genericType instanceof TypeVariable<?>) {
                    throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
                }
            }
        }
        return fields;
    }

    private List<Type> buildComposisteChild(RecordComponent attr, Class<?> recordClass, Set<Class<?>> visited) {
        validateNotVisitedRecord(recordClass, visited);
        List<Type> fields = createGroupFields(recordClass, visited);
        visited.remove(recordClass);
        return fields;
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

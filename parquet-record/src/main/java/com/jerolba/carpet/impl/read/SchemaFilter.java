package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.impl.AliasField.getFieldName;
import static com.jerolba.carpet.impl.NotNullField.isNotNull;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static java.util.stream.Collectors.toMap;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;

import java.lang.reflect.RecordComponent;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.CarpetReadConfiguration;
import com.jerolba.carpet.RecordTypeConversionException;

public class SchemaFilter {

    private final GroupType schema;
    private final CarpetReadConfiguration configuration;

    public SchemaFilter(CarpetReadConfiguration configuration, GroupType schema) {
        this.schema = schema;
        this.configuration = configuration;
    }

    public GroupType filter(Class<?> readClass) {
        if (!readClass.isRecord()) {
            throw new RecordTypeConversionException(readClass.getName() + " is not a Java Record");
        }

        List<Type> fields = schema.getFields();
        Map<String, Type> fieldsByName = fields.stream().collect(toMap(Type::getName, f -> f));

        Map<String, Type> inProjection = new HashMap<>();
        for (RecordComponent recordComponent : readClass.getRecordComponents()) {
            String name = getFieldName(recordComponent);
            Type parquetType = fieldsByName.get(name);
            if (parquetType == null) {
                if (!configuration.isIgnoreUnknown()) {
                    throw new RecordTypeConversionException(
                            "Field " + name + " of " + readClass.getName() + " class not found in parquet "
                                    + schema.getName());
                }
                continue;
            }

            // Repeated first level
            if (parquetType.isRepetition(Repetition.REPEATED)) {
                // Java field must be a collection type
                if (!Collection.class.isAssignableFrom(recordComponent.getType())) {
                    throw new RecordTypeConversionException(
                            "Repeated field " + recordComponent.getName() + " of " + readClass.getName()
                                    + " is not a collection");
                }
                var parameterized = getParameterizedCollection(recordComponent);
                if (parameterized.isCollection() || parameterized.isMap()) {
                    // Is Java child recursive collection or map?

                } else if (parquetType.isPrimitive()) {
                    // if collection type is Java "primitive"
                    var primitiveType = parquetType.asPrimitiveType();
                    var actualCollectionType = parameterized.getActualType();
                    validatePrimitiveCompatibility(primitiveType, actualCollectionType);
                    inProjection.put(name, parquetType);
                    continue;
                } else {
                    // if collection type is Java "Record"
                    var asGroupType = parquetType.asGroupType();
                    var actualCollectionType = parameterized.getActualType();
                    if (actualCollectionType.isRecord()) {
                        SchemaFilter recordFilter = new SchemaFilter(configuration, asGroupType);
                        GroupType recordSchema = recordFilter.filter(actualCollectionType);
                        inProjection.put(name, recordSchema);
                        continue;
                    }
                    throw new RecordTypeConversionException("Field " + name + " of type "
                            + actualCollectionType.getName() + " is not a basic type or " + "a Java record");
                }
            }

            // Optional or Required
            if (parquetType.isPrimitive()) {
                PrimitiveType primitiveType = parquetType.asPrimitiveType();
                validatePrimitiveCompatibility(primitiveType, recordComponent.getType());
                validateNullability(primitiveType, recordComponent);
                inProjection.put(name, parquetType);
            } else {
                GroupType asGroupType = parquetType.asGroupType();
                if (recordComponent.getType().isRecord()) {
                    validateNullability(asGroupType, recordComponent);
                    SchemaFilter recordFilter = new SchemaFilter(configuration, asGroupType);
                    GroupType recordSchema = recordFilter.filter(recordComponent.getType());
                    inProjection.put(name, recordSchema);
                } else {
                    throw new RecordTypeConversionException(
                            recordComponent.getType().getName() + " is not a Java Record");
                }
            }
        }
        List<Type> projection = schema.getFields().stream()
                .filter(f -> inProjection.containsKey(f.getName()))
                .map(f -> inProjection.get(f.getName()))
                .toList();
        return new GroupType(schema.getRepetition(), schema.getName(), projection);
    }

    private boolean validatePrimitiveCompatibility(PrimitiveType primitiveType, Class<?> type) {
        switch (primitiveType.getPrimitiveTypeName()) {
        case INT32:
            return validInt32Source(primitiveType, type);
        case INT64:
            return validInt64Source(primitiveType, type);
        case FLOAT:
            return validFloatSource(primitiveType, type);
        case DOUBLE:
            return validDoubleSource(primitiveType, type);
        case BOOLEAN:
            return validBooleanSource(primitiveType, type);
        case BINARY:
            return validBinarySource(primitiveType, type);
        case INT96, FIXED_LEN_BYTE_ARRAY:
            throw new RecordTypeConversionException(type + " deserialization not supported");
        }
        return false;
    }

    private boolean validateNullability(Type parquetType, RecordComponent recordComponent) {
        boolean isNotNull = isNotNull(recordComponent);
        if (isNotNull && parquetType.getRepetition() == Repetition.OPTIONAL) {
            Class<?> type = recordComponent.getType();
            throw new RecordTypeConversionException(
                    parquetType.getName() + " (" + type.getName() + ") can not be null");
        }
        return true;
    }

    private boolean validInt32Source(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return true;
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return true;
        }
        if (!configuration.isStrictNumericType()) {
            if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
                return true;
            }
            if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
                return false;
            }
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validInt64Source(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return true;
        }
        if (!configuration.isStrictNumericType()) {
            if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
                return false;
            }
            if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
                return true;
            }
            if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
                return false;
            }
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validFloatSource(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return true;
        }
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return true;
        }
        if (!configuration.isStrictNumericType()) {
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validDoubleSource(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return true;
        }
        if (!configuration.isStrictNumericType()) {
            if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
                return true;
            }
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validBooleanSource(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return true;
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validBinarySource(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        if (logicalType.equals(stringType()) && typeName.equals("java.lang.String")) {
            return true;
        }
        if (logicalType.equals(enumType()) && (typeName.equals("java.lang.String") || type.isEnum())) {
            return true;
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean throwInvalidConversionException(PrimitiveType primitiveType, Class<?> type) {
        throw new RecordTypeConversionException(
                primitiveType.getPrimitiveTypeName().name() + " (" + primitiveType.getName()
                        + ") can not be converted to "
                        + type.getName());
    }
}

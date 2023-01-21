package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.impl.AliasField.getFieldName;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static java.util.stream.Collectors.toMap;

import java.lang.reflect.RecordComponent;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.RecordTypeConversionException;

public class SchemaFilter {

    private final GroupType schema;
    private final SchemaValidation validation;

    public SchemaFilter(SchemaValidation validation, GroupType schema) {
        this.schema = schema;
        this.validation = validation;
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
                if (!validation.isIgnoreUnknown()) {
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
                    validation.validatePrimitiveCompatibility(primitiveType, actualCollectionType);
                    inProjection.put(name, parquetType);
                    continue;
                } else {
                    // if collection type is Java "Record"
                    var asGroupType = parquetType.asGroupType();
                    var actualCollectionType = parameterized.getActualType();
                    if (actualCollectionType.isRecord()) {
                        SchemaFilter recordFilter = new SchemaFilter(validation, asGroupType);
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
                validation.validatePrimitiveCompatibility(primitiveType, recordComponent.getType());
                validation.validateNullability(primitiveType, recordComponent);
                inProjection.put(name, parquetType);
            } else {
                GroupType asGroupType = parquetType.asGroupType();
                if (recordComponent.getType().isRecord()) {
                    validation.validateNullability(asGroupType, recordComponent);
                    SchemaFilter recordFilter = new SchemaFilter(validation, asGroupType);
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

}

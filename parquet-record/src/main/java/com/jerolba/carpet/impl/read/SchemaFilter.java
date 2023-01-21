package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.impl.AliasField.getFieldName;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static java.util.stream.Collectors.toMap;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

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

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;

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
                    throw new RecordTypeConversionException("Field " + name + " of " + readClass.getName()
                            + " class not found in parquet " + schema.getName());
                }
                continue;
            }

            if (parquetType.isRepetition(Repetition.REPEATED)) {
                Type type = analyzeOneLevelStructure(readClass, recordComponent, parquetType);
                inProjection.put(name, type);
                continue;
            }

            if (parquetType.isPrimitive()) {
                PrimitiveType primitiveType = parquetType.asPrimitiveType();
                validation.validatePrimitiveCompatibility(primitiveType, recordComponent.getType());
                validation.validateNullability(primitiveType, recordComponent);
                inProjection.put(name, parquetType);
                continue;
            }
            GroupType asGroupType = parquetType.asGroupType();
            LogicalTypeAnnotation typeAnnotation = parquetType.getLogicalTypeAnnotation();
            if (typeAnnotation == listType()) {
                if (Collection.class.isAssignableFrom(recordComponent.getType())) {
                    var parameterized = getParameterizedCollection(recordComponent);
                    Type type = analyzeMultipleLevelStructure(readClass, name, parameterized, asGroupType);
                    inProjection.put(name, type);
                    continue;
                } else {
                    throw new RecordTypeConversionException("Field " + name + " of " + readClass.getName()
                            + " is not a collection");
                }
            } else if (typeAnnotation == mapType()) {
                System.out.println("map");
            }

            if (recordComponent.getType().isRecord()) {
                validation.validateNullability(parquetType, recordComponent);
                SchemaFilter recordFilter = new SchemaFilter(validation, asGroupType);
                GroupType recordSchema = recordFilter.filter(recordComponent.getType());
                inProjection.put(name, recordSchema);
            } else {
                throw new RecordTypeConversionException(
                        recordComponent.getType().getName() + " is not a Java Record");
            }
        }
        List<Type> projection = schema.getFields().stream()
                .filter(f -> inProjection.containsKey(f.getName()))
                .map(f -> inProjection.get(f.getName()))
                .toList();
        return new GroupType(schema.getRepetition(), schema.getName(), projection);
    }

    private Type analyzeOneLevelStructure(Class<?> readClass, RecordComponent recordComponent, Type parquetType) {
        // Java field must be a collection type
        if (!Collection.class.isAssignableFrom(recordComponent.getType())) {
            throw new RecordTypeConversionException("Repeated field " + recordComponent.getName() + " of "
                    + readClass.getName() + " is not a collection");
        }
        var parameterized = getParameterizedCollection(recordComponent);
        if (parameterized.isCollection() || parameterized.isMap()) {
            // Is Java child recursive collection or map?
            throw new RecordTypeConversionException(
                    "1-level collections can no embed nested collections (List<List<?>>)");
        }
        if (parquetType.isPrimitive()) {
            // if collection type is Java "primitive"
            var primitiveType = parquetType.asPrimitiveType();
            var actualCollectionType = parameterized.getActualType();
            validation.validatePrimitiveCompatibility(primitiveType, actualCollectionType);
            return parquetType;
        }
        // if collection type is Java "Record"
        var asGroupType = parquetType.asGroupType();
        var actualCollectionType = parameterized.getActualType();
        if (actualCollectionType.isRecord()) {
            SchemaFilter recordFilter = new SchemaFilter(validation, asGroupType);
            return recordFilter.filter(actualCollectionType);
        }
        throw new RecordTypeConversionException("Field " + getFieldName(recordComponent) + " of type "
                + actualCollectionType.getName() + " is not a basic type or " + "a Java record");
    }

    private Type analyzeMultipleLevelStructure(Class<?> readClass, String name, ParameterizedCollection parameterized,
            GroupType groupType) {
        if (groupType.getFieldCount() > 1) {
            throw new RecordTypeConversionException(
                    "Nestd list " + groupType.getName() + " must have only one item");
        }
        Type groupChild = groupType.getFields().get(0);
        if (!groupChild.isRepetition(Repetition.REPEATED)) {
            throw new RecordTypeConversionException("Nestd list element " + groupChild.getName() + " must be REPEATED");
        }
        if (isThreeLevel(groupChild)) {
            return analyzeThreeLevelStructure(readClass, name, parameterized, groupType, groupChild);
        }
        return analyzeTwoLevelStructure(readClass, name, parameterized, groupType, groupChild);
    }

    private Type analyzeTwoLevelStructure(Class<?> readClass, String name, ParameterizedCollection parameterized,
            GroupType parentGroupType, Type groupChild) {
        if (parameterized.isCollection() || parameterized.isMap()) {
            // Is Java child recursive collection or map?
            LogicalTypeAnnotation typeAnnotation = groupChild.getLogicalTypeAnnotation();
            if (typeAnnotation == listType()) {
                if (parameterized.isCollection()) {
                    var parameterizedChild = parameterized.getParametizedAsCollection();
                    Type type = analyzeMultipleLevelStructure(readClass, name, parameterizedChild,
                            groupChild.asGroupType());
                    return parentGroupType.withNewFields(type);
                } else {
                    throw new RecordTypeConversionException("Field " + name + " of " + readClass.getName()
                            + " is not a collection");
                }
            } else if (typeAnnotation == mapType()) {
                System.out.println("map");
            }
        }
        if (groupChild.isPrimitive()) {
            var primitiveType = groupChild.asPrimitiveType();
            var actualCollectionType = parameterized.getActualType();
            validation.validatePrimitiveCompatibility(primitiveType, actualCollectionType);
            return parentGroupType;
        }
        var actualCollectionType = parameterized.getActualType();
        if (actualCollectionType.isRecord()) {
            SchemaFilter recordFilter = new SchemaFilter(validation, groupChild.asGroupType());
            GroupType childMapped = recordFilter.filter(actualCollectionType);
            return parentGroupType.withNewFields(childMapped);
        }
        throw new RecordTypeConversionException("Field " + name + " of type "
                + actualCollectionType.getName() + " is not a basic type or " + "a Java record");
    }

    private Type analyzeThreeLevelStructure(Class<?> readClass, String name, ParameterizedCollection parameterized,
            GroupType parentGroupType, Type listGroupType) {

        GroupType listGroupRepeated = listGroupType.asGroupType();
        Type groupChild = listGroupRepeated.getFields().get(0);

        if (parameterized.isCollection() || parameterized.isMap()) {
            LogicalTypeAnnotation typeAnnotation = groupChild.getLogicalTypeAnnotation();
            if (typeAnnotation == listType()) {
                if (parameterized.isCollection()) {
                    var parameterizedChild = parameterized.getParametizedAsCollection();
                    Type type = analyzeMultipleLevelStructure(readClass, name, parameterizedChild,
                            groupChild.asGroupType());
                    GroupType filtered = listGroupRepeated.withNewFields(type);
                    return parentGroupType.withNewFields(filtered);
                } else {
                    throw new RecordTypeConversionException("Field " + name + " of " + readClass.getName()
                            + " is not a collection");
                }
            } else if (typeAnnotation == mapType()) {
                System.out.println("map");
            }
        }
        if (groupChild.isPrimitive()) {
            var primitiveType = groupChild.asPrimitiveType();
            var actualCollectionType = parameterized.getActualType();
            validation.validatePrimitiveCompatibility(primitiveType, actualCollectionType);
            return parentGroupType;
        }
        var actualCollectionType = parameterized.getActualType();
        if (actualCollectionType.isRecord()) {
            SchemaFilter recordFilter = new SchemaFilter(validation, groupChild.asGroupType());
            GroupType childMapped = recordFilter.filter(actualCollectionType);

            var listGroupMapped = listGroupRepeated.withNewFields(childMapped);
            return parentGroupType.withNewFields(listGroupMapped);
        }
        throw new RecordTypeConversionException("Field " + name + " of type "
                + actualCollectionType.getName() + " is not a basic type or " + "a Java record");
    }

    private boolean isThreeLevel(Type child) {
        // <list-repetition> group <name> (LIST) {
        // repeated group list { <--child
        // <element-repetition> <element-type> element;
        // }
        // }
        if (child.isPrimitive()) {
            return false;
        }
        GroupType asGroup = child.asGroupType();
        if (!asGroup.getName().equals("list")) { // TODO: make configurable
            return false;
        }
        if (asGroup.getFieldCount() > 1) {
            return false;
        }
        Type grandChild = asGroup.getFields().get(0);
        if (!grandChild.getName().equals("element")) { // TODO: make configurable
            return false;
        }
        return true;
    }

}

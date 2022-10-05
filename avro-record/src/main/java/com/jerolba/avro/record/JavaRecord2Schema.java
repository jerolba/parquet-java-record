/**
 * Copyright 2022 Jerónimo López Bezanilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jerolba.avro.record;

import static com.jerolba.avro.record.AliasField.getFieldName;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseFieldTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.FieldDefault;
import org.apache.avro.SchemaBuilder.TypeBuilder;

public class JavaRecord2Schema {

    public Schema build(Class<?> recordClass) {
        Set<Class<?>> visited = new HashSet<>();
        return build(recordClass, visited);
    }

    private Schema build(Class<?> recordClass, Set<Class<?>> visited) {
        if (!recordClass.isRecord()) {
            throw new RuntimeException(recordClass.getName() + " must be a java Record");
        }
        if (visited.contains(recordClass)) {
            throw new RuntimeException("Recusive records are not supported");
        }
        visited.add(recordClass);

        var namespace = recordClass.getPackageName().replaceAll("\\$", ".");
        var record = recordClass.getSimpleName().replaceAll("\\$", ".");
        FieldAssembler<Schema> fields = SchemaBuilder.builder()
                .record(record)
                .namespace(namespace)
                .fields();

        for (var attr : recordClass.getRecordComponents()) {
            var fieldBuilder = fields.name(getFieldName(attr));
            Class<?> type = attr.getType();

            Function<BaseFieldTypeBuilder<Schema>, FieldDefault<Schema, ?>> typeDef = buildTypeDef(type);
            if (typeDef != null) {
                BaseFieldTypeBuilder<Schema> beginType;
                if (!type.isPrimitive()) {
                    beginType = fieldBuilder.type().nullable();
                } else {
                    beginType = fieldBuilder.type();
                }
                fields = typeDef.apply(beginType).noDefault();
            } else if (type.isEnum()) {
                fields = fieldBuilder.type().nullable().enumeration(type.getSimpleName())
                        .symbols(enumSymbols(type)).noDefault();
            } else {
                fields = compositeSchema(attr, fieldBuilder, visited);
            }
        }
        visited.remove(recordClass);
        return fields.endRecord();
    }

    private String[] enumSymbols(Class<?> type) {
        Object[] enumConstants = ((Class) type).getEnumConstants();
        return Stream.of(enumConstants).map(Object::toString).toArray(String[]::new);
    }

    private Function<BaseFieldTypeBuilder<Schema>, FieldDefault<Schema, ?>> buildTypeDef(Class<?> type) {
        return switch (type.getName()) {
        case "short", "java.lang.Short", "int", "java.lang.Integer" -> BaseFieldTypeBuilder::intType;
        case "byte", "java.lang.Byte" -> BaseFieldTypeBuilder::intType;
        case "long", "java.lang.Long" -> BaseFieldTypeBuilder::longType;
        case "float", "java.lang.Float" -> BaseFieldTypeBuilder::floatType;
        case "double", "java.lang.Double" -> BaseFieldTypeBuilder::doubleType;
        case "boolean", "java.lang.Boolean" -> BaseFieldTypeBuilder::booleanType;
        case "java.lang.String" -> BaseFieldTypeBuilder::stringType;
        default -> null;
        };
    }

    private FieldAssembler<Schema> compositeSchema(RecordComponent attr, FieldBuilder<Schema> fieldBuilder,
            Set<Class<?>> visited) {

        Class<?> attrType = attr.getType();
        if (attrType.isRecord()) {
            Schema subSchema = build(attrType, visited);
            return fieldBuilder.type().unionOf().nullType().and().type(subSchema).endUnion().noDefault();
        }
        Type genericType = attr.getGenericType();
        if (genericType instanceof Class<?>) {
            throw new RuntimeException(genericType.toString() + " is not a Java record");
        }
        if (genericType instanceof TypeVariable<?>) {
            throw new RuntimeException(genericType.toString() + " generic types not supported");
        }
        if (genericType instanceof ParameterizedType paramType) {
            Schema subSchema = childCollection(paramType, visited);
            return fieldBuilder.type().unionOf().nullType().and().type(subSchema).endUnion().noDefault();
        }
        return null;
    }

    private Class<?> getChildCollectionType(ParameterizedType paramType) {
        Class<?> parametizedClass = (Class<?>) paramType.getRawType();
        if (!Collection.class.isAssignableFrom(parametizedClass)) {
            throw new RuntimeException("Invalid collection type " + paramType.getRawType());
        }
        Type listType = paramType.getActualTypeArguments()[0];
        if (!(listType instanceof Class<?>)) {
            throw new RuntimeException("Invalid type " + parametizedClass + " as " + listType);
        }
        return (Class<?>) listType;
    }

    private static final Set<Class<?>> SIMPLE_TYPES = Set.of(Long.class, Integer.class, Short.class, Byte.class,
            Double.class, Float.class, Boolean.class, String.class);

    private Schema childCollection(ParameterizedType genericType, Set<Class<?>> visited) {
        Class<?> childCollectionType = getChildCollectionType(genericType);
        if (SIMPLE_TYPES.contains(childCollectionType)) {
            TypeBuilder<Schema> items = SchemaBuilder.builder().array().items();
            return simpleTypeItems(items, childCollectionType);
        }
        if (childCollectionType.isEnum()) {
            return SchemaBuilder.builder().array().items().enumeration(childCollectionType.getSimpleName())
                    .symbols(enumSymbols(childCollectionType));
        }
        Schema childSchema = build(childCollectionType, visited);
        return SchemaBuilder.builder().array().items(childSchema);
    }

    private Schema simpleTypeItems(TypeBuilder<Schema> items, Class<?> type) {
        return switch (type.getName()) {
        case "java.lang.Short", "java.lang.Integer", "java.lang.Byte" -> items.intType();
        case "java.lang.Long" -> items.longType();
        case "java.lang.Float" -> items.floatType();
        case "java.lang.Double" -> items.doubleType();
        case "java.lang.Boolean" -> items.booleanType();
        case "java.lang.String" -> items.stringType();
        default -> null;
        };
    }

}

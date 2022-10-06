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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jerolba.record.Alias;

public class JavaRecordToAvroSchemaTest {

    public record PrimitivesAndObjects(String name,
            int intPrimitive, Integer intObject,
            long longPrimitive, Long longObject,
            float floatPrimitive, Float floatObject,
            double doublePrimitive, Double doubleObject,
            boolean booleanPrimitive, Boolean booleanObject) {
    }

    JavaRecord2Schema recordToSchema = new JavaRecord2Schema();

    @Test
    void basicTypesSchema() throws IOException {
        Schema schema = recordToSchema.build(PrimitivesAndObjects.class);
        String expected = """
                {
                    "type": "record",
                    "name": "PrimitivesAndObjects",
                    "namespace": "com.jerolba.avro.record",
                    "fields": [
                        {
                            "name": "name",
                            "type": ["string", "null"]
                        }, {
                            "name": "intPrimitive",
                            "type": "int"
                        }, {
                            "name": "intObject",
                            "type": ["int", "null"]
                        }, {
                            "name": "longPrimitive",
                            "type": "long"
                        }, {
                            "name": "longObject",
                            "type": ["long", "null"]
                        }, {
                            "name": "floatPrimitive",
                            "type": "float"
                        }, {
                            "name": "floatObject",
                            "type": ["float", "null"]
                        }, {
                            "name": "doublePrimitive",
                            "type": "double"
                        }, {
                            "name": "doubleObject",
                            "type": ["double", "null"]
                        }, {
                            "name": "booleanPrimitive",
                            "type": "boolean"
                        }, {
                            "name": "booleanObject",
                            "type": ["boolean", "null"]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    public record CompositeChild(String id, int value) {
    }

    public record CompositeMain(String name, CompositeChild child) {
    }

    @Test
    void compositeObjects() throws IOException {
        Schema schema = recordToSchema.build(CompositeMain.class);
        String expected = """
                {
                    "type": "record",
                    "name": "CompositeMain",
                    "namespace": "com.jerolba.avro.record",
                    "fields": [
                        {
                            "name": "name",
                            "type": ["string", "null"]
                        },
                        {
                            "name": "child",
                            "type": [
                                "null",
                                {
                                    "type": "record",
                                    "name": "CompositeChild",
                                    "fields": [
                                        {
                                            "name": "id",
                                            "type": ["string", "null"]
                                        }, {
                                            "name": "value",
                                            "type": "int"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    public record CompositeGeneric<T> (String name, T child) {
    }

    @Test
    void compositeGenericObjectsAreNotAllowed() throws IOException {
        assertThrows(RuntimeException.class, () -> recordToSchema.build(CompositeGeneric.class));
    }

    public record RecursiveLoop(String id, RercursiveMain recursive) {
    }

    public record RecursiveChild(String id, RecursiveLoop child) {
    }

    public record RercursiveMain(String name, RecursiveChild child) {
    }

    @Test
    void recursiveTypesAreNotAllowed() throws IOException {
        assertThrows(RuntimeException.class, () -> recordToSchema.build(RercursiveMain.class));
    }

    public enum OrgType {
        FOO, BAR, BAZ
    }

    public record WithEnum(String name, OrgType orgType) {

    }

    @Test
    void withEnum() throws IOException {
        Schema schema = recordToSchema.build(WithEnum.class);
        String expected = """
                {
                    "type": "record",
                    "name": "WithEnum",
                    "namespace": "com.jerolba.avro.record",
                    "fields": [
                        {
                            "name": "name",
                            "type": ["string", "null"]
                        },
                        {
                            "name": "orgType",
                            "type": [
                                {
                                    "type": "enum",
                                    "name": "OrgType",
                                    "symbols": ["FOO", "BAR", "BAZ"]
                                },
                                "null"
                            ]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    public record CompositeMainCollection(String name, List<CompositeChild> child) {
    }

    @Test
    void compositeCollection() throws IOException {
        Schema schema = recordToSchema.build(CompositeMainCollection.class);
        String expected = """
                {
                    "type": "record",
                    "name": "CompositeMainCollection",
                    "namespace": "com.jerolba.avro.record",
                    "fields": [
                        {
                            "name": "name",
                            "type": [
                                "string",
                                "null"
                            ]
                        },
                        {
                            "name": "child",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "CompositeChild",
                                        "fields": [
                                            {
                                                "name": "id",
                                                "type": [
                                                    "string",
                                                    "null"
                                                ]
                                            },
                                            {
                                                "name": "value",
                                                "type": "int"
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    public record SimpleCollection(String name, List<Integer> sizes, List<String> names, List<OrgType> types) {
    }

    @Test
    void simpleCollection() throws IOException {
        Schema schema = recordToSchema.build(SimpleCollection.class);
        System.out.println(schema);
        String expected = """
                {
                    "type": "record",
                    "name": "SimpleCollection",
                    "namespace": "com.jerolba.avro.record",
                    "fields": [
                        {
                            "name": "name",
                            "type": [
                                "string",
                                "null"
                            ]
                        },
                        {
                            "name": "sizes",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "int"
                                }
                            ]
                        },
                        {
                            "name": "names",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ]
                        },
                        {
                            "name": "types",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "enum",
                                        "name": "OrgType",
                                        "namespace": "",
                                        "symbols": [
                                            "FOO",
                                            "BAR",
                                            "BAZ"
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    public record WithAlias(@Alias("transformed") String original) {

    }

    @Test
    void fieldAlias() throws IOException {
        Schema schema = recordToSchema.build(WithAlias.class);
        String expected = """
                {
                    "type": "record",
                    "name": "WithAlias",
                    "namespace": "com.jerolba.avro.record",
                    "fields": [
                        {
                            "name": "transformed",
                            "type": ["string", "null"]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    public record WithNonStandarNameAlias(@Alias("no_stardar_java_name") String original) {

    }

    @Test
    void nonStandarFieldAlias() throws IOException {
        Schema schema = recordToSchema.build(WithNonStandarNameAlias.class);
        String expected = """
                {
                    "type": "record",
                    "name": "WithNonStandarNameAlias",
                    "namespace": "com.jerolba.avro.record",
                    "fields": [
                        {
                            "name": "no_stardar_java_name",
                            "type": ["string", "null"]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    private void assertEqualSchema(String str, Schema schema) throws IOException {
        ObjectMapper m = new ObjectMapper();
        Map expectedMap = m.readValue(str, Map.class);
        Map actualMap = m.readValue(schema.toString(), Map.class);
        assertEquals(expectedMap, actualMap);
    }
}
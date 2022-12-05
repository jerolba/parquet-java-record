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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jerolba.record.annotation.Alias;
import com.jerolba.record.annotation.NotNull;

public class JavaRecordToAvroSchemaTest {

    JavaRecord2Schema recordToSchema = new JavaRecord2Schema();

    @Test
    void innerRecord() throws IOException {
        record InnerRecord(String name) {

        }

        Schema schema = recordToSchema.build(InnerRecord.class);
        String expected = """
                {
                    "type": "record",
                    "name": "InnerRecord",
                    "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest$",
                    "fields": [
                        {
                            "name": "name",
                            "type": [
                                "string",
                                "null"
                            ]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    record PrivateRecord(String name) {

    }

    @Test
    void privateRecord() throws IOException {
        Schema schema = recordToSchema.build(PrivateRecord.class);
        String expected = """
                {
                    "type": "record",
                    "name": "PrivateRecord",
                    "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest",
                    "fields": [
                        {
                            "name": "name",
                            "type": [
                                "string",
                                "null"
                            ]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

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
                    "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest",
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
                    "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest",
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
                    "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest",
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
                    "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest",
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

    public record ByteShort(byte fromByte, short fromShort) {

    }

    @Test
    void convertByteShortToInteger() throws IOException {
        Schema schema = recordToSchema.build(ByteShort.class);
        String expected = """
                {
                    "type": "record",
                    "name": "ByteShort",
                    "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest",
                    "fields": [
                        {
                            "name": "fromByte",
                            "type": "int"
                        },
                        {
                            "name": "fromShort",
                            "type": "int"
                        }

                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    @Test
    void fieldAlias() throws IOException {
        Schema schema = recordToSchema.build(WithAlias.class);
        String expected = """
                {
                    "type": "record",
                    "name": "WithAlias",
                    "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest",
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
                    "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest",
                    "fields": [
                        {
                            "name": "no_stardar_java_name",
                            "type": ["string", "null"]
                        }
                    ]
                }""";
        assertEqualSchema(expected, schema);
    }

    @Nested
    class NotNullAnnotated {

        // All types are not nullables
        public record NotNullableObjects(@NotNull String name,
                int intPrimitive, @NotNull Integer intObject,
                long longPrimitive, @NotNull Long longObject,
                float floatPrimitive, @NotNull Float floatObject,
                double doublePrimitive, @NotNull Double doubleObject,
                boolean booleanPrimitive, @NotNull Boolean booleanObject,
                @NotNull OrgType orgType) {
        }

        @Test
        void notNullableObjects() throws IOException {
            Schema schema = recordToSchema.build(NotNullableObjects.class);
            String expected = """
                    {
                        "type": "record",
                        "name": "NotNullableObjects",
                        "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest.NotNullAnnotated",
                        "fields": [
                            {
                                "name": "name",
                                "type": "string"
                            }, {
                                "name": "intPrimitive",
                                "type": "int"
                            }, {
                                "name": "intObject",
                                "type": "int"
                            }, {
                                "name": "longPrimitive",
                                "type": "long"
                            }, {
                                "name": "longObject",
                                "type": "long"
                            }, {
                                "name": "floatPrimitive",
                                "type": "float"
                            }, {
                                "name": "floatObject",
                                "type": "float"
                            }, {
                                "name": "doublePrimitive",
                                "type": "double"
                            }, {
                                "name": "doubleObject",
                                "type": "double"
                            }, {
                                "name": "booleanPrimitive",
                                "type": "boolean"
                            }, {
                                "name": "booleanObject",
                                "type": "boolean"
                            }, {
                                "name": "orgType",
                                "type":
                                    {
                                        "type": "enum",
                                        "name": "OrgType",
                                        "symbols": ["FOO", "BAR", "BAZ"]
                                    }
                            }
                        ]
                    }""";
            assertEqualSchema(expected, schema);
        }

        public record CompositeMainNotNull(@NotNull String name, @NotNull CompositeChildNotNull child) {
        }

        public record CompositeChildNotNull(@NotNull String id, int value) {
        }

        @Test
        void compositeObjectsNotNull() throws IOException {
            Schema schema = recordToSchema.build(CompositeMainNotNull.class);
            String expected = """
                    {
                        "type": "record",
                        "name": "CompositeMainNotNull",
                        "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest.NotNullAnnotated",
                        "fields": [
                            {
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "name": "child",
                                "type":
                                    {
                                        "type": "record",
                                        "name": "CompositeChildNotNull",
                                        "fields": [
                                            {
                                                "name": "id",
                                                "type": "string"
                                            }, {
                                                "name": "value",
                                                "type": "int"
                                            }
                                        ]
                                    }
                            }
                        ]
                    }""";
            assertEqualSchema(expected, schema);
        }

        public record CompositeMainNotNullCollection(@NotNull String name, @NotNull List<CompositeChildNotNull> child) {
        }

        @Test
        void compositeNotNullCollection() throws IOException {
            Schema schema = recordToSchema.build(CompositeMainNotNullCollection.class);
            String expected = """
                    {
                        "type": "record",
                        "name": "CompositeMainNotNullCollection",
                        "namespace": "com.jerolba.avro.record.JavaRecordToAvroSchemaTest.NotNullAnnotated",
                        "fields": [
                            {
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "name": "child",
                                "type":
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "record",
                                            "name": "CompositeChildNotNull",
                                            "fields": [
                                                {
                                                    "name": "id",
                                                    "type": "string"
                                                },
                                                {
                                                    "name": "value",
                                                    "type": "int"
                                                }
                                            ]
                                        }
                                    }
                            }
                        ]
                    }""";
            assertEqualSchema(expected, schema);
        }
    }

    private void assertEqualSchema(String str, Schema schema) throws IOException {
        ObjectMapper m = new ObjectMapper();
        Map expectedMap = m.readValue(str, Map.class);
        Map actualMap = m.readValue(schema.toString(), Map.class);
        assertEquals(expectedMap, actualMap);
    }
}

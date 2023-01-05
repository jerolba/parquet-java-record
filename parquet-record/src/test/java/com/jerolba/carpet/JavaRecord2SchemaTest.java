package com.jerolba.carpet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.record.annotation.Alias;
import com.jerolba.record.annotation.NotNull;

class JavaRecord2SchemaTest {

    private final CarpetConfiguration default3Levels = new CarpetConfiguration(AnnotatedLevels.THREE);

    @Test
    void simpleRecordTest() {
        record SimpleRecord(long id, String name) {
        }

        MessageType schema = new JavaRecord2Schema(default3Levels).createSchema(SimpleRecord.class);
        String expected = """
                message SimpleRecord {
                  required int64 id;
                  optional binary name (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Test
    void privitiveTypesRecordTest() {
        record PrimitiveTypesRecord(long longValue, int intValue, float floatValue, double doubleValue,
                short shortValue, byte byteValue, boolean booleanValue) {
        }

        MessageType schema = new JavaRecord2Schema(default3Levels).createSchema(PrimitiveTypesRecord.class);
        String expected = """
                message PrimitiveTypesRecord {
                  required int64 longValue;
                  required int32 intValue;
                  required float floatValue;
                  required double doubleValue;
                  required int32 shortValue;
                  required int32 byteValue;
                  required boolean booleanValue;
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Test
    void privitiveObjectsTypesRecordTest() {
        record PrimitiveObjectsTypesRecord(Long longValue, Integer intValue, Float floatValue, Double doubleValue,
                Short shortValue, Byte byteValue, Boolean booleanValue) {
        }

        MessageType schema = new JavaRecord2Schema(default3Levels).createSchema(PrimitiveObjectsTypesRecord.class);
        String expected = """
                message PrimitiveObjectsTypesRecord {
                  optional int64 longValue;
                  optional int32 intValue;
                  optional float floatValue;
                  optional double doubleValue;
                  optional int32 shortValue;
                  optional int32 byteValue;
                  optional boolean booleanValue;
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Test
    void fieldAliasRecordTest() {
        record FieldAliasRecord(long id, @Alias("nombre") String name) {
        }

        MessageType schema = new JavaRecord2Schema(default3Levels).createSchema(FieldAliasRecord.class);
        String expected = """
                message FieldAliasRecord {
                  required int64 id;
                  optional binary nombre (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Test
    void notNullFieldRecordTest() {
        record NotNullFieldRecord(@NotNull Long id, @NotNull String name) {
        }

        MessageType schema = new JavaRecord2Schema(default3Levels).createSchema(NotNullFieldRecord.class);
        String expected = """
                message NotNullFieldRecord {
                  required int64 id;
                  required binary name (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Nested
    class SimpleComposition {

        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema(default3Levels);

        @Test
        void simpleParentChildRecordTest() {
            record ChildRecord(String key, int value) {
            }
            record ParentRecord(long id, String name, ChildRecord foo) {
            }

            MessageType schema = schemaFactory.createSchema(ParentRecord.class);
            String expected = """
                    message ParentRecord {
                      required int64 id;
                      optional binary name (STRING);
                      optional group foo {
                        optional binary key (STRING);
                        required int32 value;
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void notNullChildRecordTest() {
            record ChildRecord(String key, int value) {
            }
            record NotNullChildRecord(long id, String name, @NotNull @Alias("bar") ChildRecord foo) {
            }

            MessageType schema = schemaFactory.createSchema(NotNullChildRecord.class);
            String expected = """
                    message NotNullChildRecord {
                      required int64 id;
                      optional binary name (STRING);
                      required group bar {
                        optional binary key (STRING);
                        required int32 value;
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void recursivityIsNotAllowed() {
            record Recursive(long id, String name, Recursive child) {
            }

            assertThrows(RecordTypeConversionException.class, () -> schemaFactory.createSchema(Recursive.class));
        }

        public record RecursiveChild(String name, RecursiveTransitive child) {
        }

        public record RecursiveTransitive(long id, String name, RecursiveChild child) {
        }

        @Test
        void transitiveRecursivityIsNotAllowed() {
            record Recursive(long id, String name, Recursive child) {
            }

            assertThrows(RecordTypeConversionException.class, () -> schemaFactory.createSchema(Recursive.class));
        }

        @Test
        void genericAreNotAllowed() {
            record WithGeneric<T>(String name, T child) {
            }

            assertThrows(RecordTypeConversionException.class, () -> schemaFactory.createSchema(WithGeneric.class));
        }

    }

    @Nested
    class EnumTypes {

        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema(default3Levels);

        enum Status {
            ACTIVE, INACTIVE, DELETED
        }

        @Test
        void withEnum() {
            record WithEnum(long id, String name, Status status) {
            }

            MessageType schema = schemaFactory.createSchema(WithEnum.class);
            String expected = """
                    message WithEnum {
                      required int64 id;
                      optional binary name (STRING);
                      optional binary status (ENUM);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void withNotNullEnum() {
            record WithNotNullEnum(long id, String name, @NotNull @Alias("state") Status status) {
            }

            MessageType schema = schemaFactory.createSchema(WithNotNullEnum.class);
            String expected = """
                    message WithNotNullEnum {
                      required int64 id;
                      optional binary name (STRING);
                      required binary state (ENUM);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

    }

    @Nested
    class NestedCollection1Level {

        private final CarpetConfiguration oneLevel = new CarpetConfiguration(AnnotatedLevels.ONE);
        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema(oneLevel);

        @Test
        void nestedSimpleTypeCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated int32 values;
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedCollections() {
            record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
            }

            assertThrows(RecordTypeConversionException.class,
                    () -> schemaFactory.createSchema(ConsecutiveNestedCollection.class));
        }

        @Test
        void nonConsecutiveNestedCollections() {
            record ChildCollection(String name, List<String> alias) {

            }
            record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
            }

            MessageType schema = schemaFactory.createSchema(NonConsecutiveNestedCollection.class);
            String expected = """
                    message NonConsecutiveNestedCollection {
                      optional binary id (STRING);
                      repeated group values {
                        optional binary name (STRING);
                        repeated binary alias (STRING);
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedGenericIsNotSupported() {
            record GenericCollection<T>(String id, List<T> values) {
            }
            assertThrows(RecordTypeConversionException.class,
                    () -> schemaFactory.createSchema(GenericCollection.class));
        }

    }

    @Nested
    class NestedCollection2Level {

        private final CarpetConfiguration twoLevel = new CarpetConfiguration(AnnotatedLevels.TWO);
        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema(twoLevel);

        @Test
        void nestedSimpleTypeCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated int32 element;
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedRecordCollection() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record NestedRecordCollection(String id, List<ChildRecord> values) {
            }

            MessageType schema = schemaFactory.createSchema(NestedRecordCollection.class);
            String expected = """
                    message NestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element {
                          optional binary id (STRING);
                          optional boolean loaded;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedRecordCollection() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record ConsecutiveNestedRecordCollection(String id, List<List<ChildRecord>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedRecordCollection.class);
            String expected = """
                    message ConsecutiveNestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element (LIST) {
                          repeated group element {
                            optional binary id (STRING);
                            optional boolean loaded;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedRecordCollection() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record ConsecutiveTripleNestedRecordCollection(String id, List<List<List<ChildRecord>>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedRecordCollection.class);
            String expected = """
                    message ConsecutiveTripleNestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element (LIST) {
                          repeated group element (LIST) {
                            repeated group element {
                              optional binary id (STRING);
                              optional boolean loaded;
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedSimpleTypeCollections() {
            record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedCollection.class);
            String expected = """
                    message ConsecutiveNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element (LIST) {
                          repeated int32 element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedSimpleTypeCollections() {
            record ConsecutiveTripleNestedCollection(String id, List<List<List<Integer>>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedCollection.class);
            String expected = """
                    message ConsecutiveTripleNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element (LIST) {
                          repeated group element (LIST) {
                            repeated int32 element;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nonConsecutiveNestedCollections() {
            record ChildCollection(String name, List<String> alias) {

            }
            record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
            }

            MessageType schema = schemaFactory.createSchema(NonConsecutiveNestedCollection.class);
            String expected = """
                    message NonConsecutiveNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element {
                          optional binary name (STRING);
                          optional group alias (LIST) {
                            repeated binary element (STRING);
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedGenericIsNotSupported() {
            record GenericCollection<T>(String id, List<T> values) {
            }
            assertThrows(RecordTypeConversionException.class,
                    () -> schemaFactory.createSchema(GenericCollection.class));
        }

    }

    @Nested
    class NestedCollection3Level {

        private final CarpetConfiguration threeLevel = new CarpetConfiguration(AnnotatedLevels.THREE);
        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema(threeLevel);

        @Test
        void nestedSimpleTypeCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional int32 element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedRecordCollection() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record NestedRecordCollection(String id, List<ChildRecord> values) {
            }

            MessageType schema = schemaFactory.createSchema(NestedRecordCollection.class);
            String expected = """
                    message NestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element {
                            optional binary id (STRING);
                            optional boolean loaded;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedRecordCollection() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record ConsecutiveNestedRecordCollection(String id, List<List<ChildRecord>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedRecordCollection.class);
            String expected = """
                    message ConsecutiveNestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional group element {
                                optional binary id (STRING);
                                optional boolean loaded;
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedRecordCollection() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record ConsecutiveTripleNestedRecordCollection(String id, List<List<List<ChildRecord>>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedRecordCollection.class);
            String expected = """
                    message ConsecutiveTripleNestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional group element (LIST) {
                                repeated group list {
                                  optional group element {
                                    optional binary id (STRING);
                                    optional boolean loaded;
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedSimpleTypeCollections() {
            record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedCollection.class);
            String expected = """
                    message ConsecutiveNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional int32 element;
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedSimpleTypeCollections() {
            record ConsecutiveTripleNestedCollection(String id, List<List<List<Integer>>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedCollection.class);
            String expected = """
                    message ConsecutiveTripleNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional group element (LIST) {
                                repeated group list {
                                  optional int32 element;
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nonConsecutiveNestedCollections() {
            record ChildCollection(String name, List<String> alias) {

            }
            record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
            }

            MessageType schema = schemaFactory.createSchema(NonConsecutiveNestedCollection.class);
            String expected = """
                    message NonConsecutiveNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element {
                            optional binary name (STRING);
                            optional group alias (LIST) {
                              repeated group list {
                                optional binary element (STRING);
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedGenericIsNotSupported() {
            record GenericCollection<T>(String id, List<T> values) {
            }
            assertThrows(RecordTypeConversionException.class,
                    () -> schemaFactory.createSchema(GenericCollection.class));
        }

    }
}
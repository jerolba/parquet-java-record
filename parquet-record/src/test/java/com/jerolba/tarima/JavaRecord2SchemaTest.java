package com.jerolba.tarima;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.record.annotation.Alias;
import com.jerolba.record.annotation.NotNull;

class JavaRecord2SchemaTest {

    public record SimpleRecord(long id, String name) {
    }

    @Test
    void simpleRecordTest() {
        MessageType schema = new JavaRecord2Schema().createSchema(SimpleRecord.class);
        String expected = """
                message SimpleRecord {
                  required int64 id;
                  optional binary name (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    public record PrimitiveTypesRecord(long longValue, int intValue, float floatValue, double doubleValue,
            short shortValue, byte byteValue, boolean booleanValue) {
    }

    @Test
    void privitiveTypesRecordTest() {
        MessageType schema = new JavaRecord2Schema().createSchema(PrimitiveTypesRecord.class);
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

    public record PrimitiveObjectsTypesRecord(Long longValue, Integer intValue, Float floatValue, Double doubleValue,
            Short shortValue, Byte byteValue, Boolean booleanValue) {
    }

    @Test
    void privitiveObjectsTypesRecordTest() {
        MessageType schema = new JavaRecord2Schema().createSchema(PrimitiveObjectsTypesRecord.class);
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

    public record FieldAliasRecord(long id, @Alias("nombre") String name) {
    }

    @Test
    void fieldAliasRecordTest() {
        MessageType schema = new JavaRecord2Schema().createSchema(FieldAliasRecord.class);
        String expected = """
                message FieldAliasRecord {
                  required int64 id;
                  optional binary nombre (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    public record NotNullFieldRecord(@NotNull Long id, @NotNull String name) {
    }

    @Test
    void notNullFieldRecordTest() {
        MessageType schema = new JavaRecord2Schema().createSchema(NotNullFieldRecord.class);
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

        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema();

        public record ParentRecord(long id, String name, ChildRecord foo) {
        }

        public record ChildRecord(String key, int value) {

        }

        @Test
        void simpleParentChildRecordTest() {
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

        public record NotNullChildRecord(long id, String name, @NotNull @Alias("bar") ChildRecord foo) {
        }

        @Test
        void notNullChildRecordTest() {
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

        public record Recursive(long id, String name, Recursive child) {
        }

        @Test
        void recursivityIsNotAllowed() {
            assertThrows(RecordTypeConversionException.class, () -> schemaFactory.createSchema(Recursive.class));
        }

        public record RecursiveTransitive(long id, String name, RecursiveChild child) {
        }

        public record RecursiveChild(String name, RecursiveTransitive child) {
        }

        @Test
        void transitiveRecursivityIsNotAllowed() {
            assertThrows(RecordTypeConversionException.class, () -> schemaFactory.createSchema(Recursive.class));
        }

    }

    @Nested
    class EnumTypes {

        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema();

        enum Status {
            ACTIVE, INACTIVE, DELETED
        }

        public record WithEnum(long id, String name, Status status) {

        }

        @Test
        void withEnum() {
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

        public record WithNotNullEnum(long id, String name, @NotNull @Alias("state") Status status) {

        }

        @Test
        void withNotNullEnum() {
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
}

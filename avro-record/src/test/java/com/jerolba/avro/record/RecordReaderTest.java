package com.jerolba.avro.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RecordReaderTest {

    public record PrimitivesAndObjects(String name,
            int intPrimitive, Integer intObject,
            long longPrimitive, Long longObject,
            float floatPrimitive, Float floatObject,
            double doublePrimitive, Double doubleObject,
            boolean booleanPrimitive, Boolean booleanObject) {
    }

    @Test
    void primitivesAndObjectsTypes() throws IOException {
        Schema schema = SchemaBuilder.builder()
                .record("PrimitivesAndObjects")
                .namespace("com.jerolba.avro")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("intPrimitive").type().intType().noDefault()
                .name("intObject").type().nullable().intType().noDefault()
                .name("longPrimitive").type().longType().noDefault()
                .name("longObject").type().nullable().longType().noDefault()
                .name("floatPrimitive").type().floatType().noDefault()
                .name("floatObject").type().nullable().floatType().noDefault()
                .name("doublePrimitive").type().doubleType().noDefault()
                .name("doubleObject").type().nullable().doubleType().noDefault()
                .name("booleanPrimitive").type().booleanType().noDefault()
                .name("booleanObject").type().nullable().booleanType().noDefault()
                .endRecord();

        try (DataFileWriter<GenericRecord> writer = writer("/tmp/privitiveObjects.avro", schema)) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("name", "foo");
            record.put("intPrimitive", 1);
            record.put("intObject", 2);
            record.put("longPrimitive", 3L);
            record.put("longObject", 4L);
            record.put("floatPrimitive", 5.0f);
            record.put("floatObject", 6.0f);
            record.put("doublePrimitive", 7.0);
            record.put("doubleObject", 8.0);
            record.put("booleanPrimitive", true);
            record.put("booleanObject", true);
            writer.append(record);

            record = new GenericData.Record(schema);
            record.put("name", "bar");
            record.put("intPrimitive", 10);
            record.put("longPrimitive", 30L);
            record.put("floatPrimitive", 50.0);
            record.put("doublePrimitive", 70.0);
            record.put("booleanPrimitive", true);
            writer.append(record);
        }

        AvroRecordReader<PrimitivesAndObjects> loader = new AvroRecordReader<>("/tmp/privitiveObjects.avro", PrimitivesAndObjects.class);
        Iterator<PrimitivesAndObjects> it = loader.iterator();
        assertEquals(new PrimitivesAndObjects("foo", 1, 2, 3L, 4L, 5.0F, 6.0F, 7.0, 8.0, true, true), it.next());
        assertEquals(new PrimitivesAndObjects("bar", 10, null, 30L, null, 50.0F, null, 70.0, null, true, null), it.next());
        assertFalse(it.hasNext());
    }

    @Nested
    class EnumParsing {

        public enum OrgType {
            FOO, BAR, BAZ
        }

        public record WithEnum(String name, OrgType orgType) {
        }

        private Schema schema = SchemaBuilder.builder()
                .record("WithEnum")
                .namespace("com.jerolba.avro")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("orgType").type().nullable().stringType().noDefault()
                .endRecord();

        @Test
        void withEnums() throws IOException {
            try (DataFileWriter<GenericRecord> writer = writer("/tmp/withEnum.avro", schema)) {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("orgType", OrgType.FOO.name());
                writer.append(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("orgType", null);
                writer.append(record);
            }

            AvroRecordReader<WithEnum> loader = new AvroRecordReader<>("/tmp/withEnum.avro", WithEnum.class);
            Iterator<WithEnum> it = loader.iterator();
            assertEquals(new WithEnum("Apple", OrgType.FOO), it.next());
            assertEquals(new WithEnum("Spotify", null), it.next());
            assertFalse(it.hasNext());
        }

        @Test
        void unconvertibleEnums() throws IOException {
            try (DataFileWriter<GenericRecord> writer = writer("/tmp/withEnum.avro", schema)) {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("orgType", "invalid");
                writer.append(record);
            }

            AvroRecordReader<WithEnum> loader = new AvroRecordReader<>("/tmp/withEnum.avro", WithEnum.class);
            Iterator<WithEnum> it = loader.iterator();
            assertThrows(IllegalArgumentException.class, () -> it.next());
        }
    }

    @Nested
    class CompositedClasses {

        public record CompositeChild(String id, int value) {
        }

        public record CompositeMain(String name, CompositeChild child) {
        }

        public record CompositeGeneric<T> (String name, T child) {
        }

        Schema childSchema = SchemaBuilder.builder()
                .record("CompositeChild")
                .namespace("com.jerolba.avro")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("value").type().intType().noDefault()
                .endRecord();

        @Test
        void nullableCompositeValue() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.avro")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("child").type().unionOf().nullType().and().type(childSchema).endUnion().noDefault()
                    .endRecord();

            try (DataFileWriter<GenericRecord> writer = writer("/tmp/compositeValue.avro", schema)) {

                GenericData.Record child = new GenericData.Record(childSchema);
                child.put("id", "12345");
                child.put("value", 12345);

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("child", child);
                writer.append(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                writer.append(record);
            }

            AvroRecordReader<CompositeMain> loader = new AvroRecordReader<>("/tmp/compositeValue.avro", CompositeMain.class);
            Iterator<CompositeMain> it = loader.iterator();
            assertEquals(new CompositeMain("Apple", new CompositeChild("12345", 12345)), it.next());
            assertEquals(new CompositeMain("Spotify", null), it.next());
        }

        @Test
        void notNullableCompositeValue() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.avro")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("child").type(childSchema).noDefault()
                    .endRecord();

            try (DataFileWriter<GenericRecord> writer = writer("/tmp/compositeValue.avro", schema)) {
                GenericData.Record child = new GenericData.Record(childSchema);
                child.put("id", "12345");
                child.put("value", 12345);

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("child", child);
                writer.append(record);
            }

            AvroRecordReader<CompositeMain> loader = new AvroRecordReader<>("/tmp/compositeValue.avro", CompositeMain.class);
            Iterator<CompositeMain> it = loader.iterator();
            assertEquals(new CompositeMain("Apple", new CompositeChild("12345", 12345)), it.next());
        }

        @Test
        void genericCompositeNotSupported() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.avro")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("child").type(childSchema).noDefault()
                    .endRecord();

            try (DataFileWriter<GenericRecord> writer = writer("/tmp/compositeValue.avro", schema)) {
                GenericData.Record child = new GenericData.Record(childSchema);
                child.put("id", "12345");
                child.put("value", 12345);

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("child", child);
                writer.append(record);
            }

            AvroRecordReader<CompositeGeneric> loader = new AvroRecordReader<>("/tmp/compositeValue.avro", CompositeGeneric.class);
            assertThrows(IllegalArgumentException.class, () -> loader.iterator());
        }

    }

    @Nested
    class CompositedListClasses {

        public record CompositeChild(String id, int value) {
        }

        public record CompositeMain(String name, List<CompositeChild> children) {
        }

        Schema childSchema = SchemaBuilder.builder()
                .record("CompositeChild")
                .namespace("com.jerolba.avro")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("value").type().intType().noDefault()
                .endRecord();
        Schema childrenSchema = SchemaBuilder.builder().array().items(childSchema);

        @Test
        void nonNullableCompositeListValue() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.avro")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("children").type(childrenSchema).noDefault()
                    .endRecord();

            try (DataFileWriter<GenericRecord> writer = writer("/tmp/compositeListValue.avro", schema)) {

                GenericData.Record child1 = new GenericData.Record(childSchema);
                child1.put("id", "12345");
                child1.put("value", 12345);

                GenericData.Record child2 = new GenericData.Record(childSchema);
                child2.put("id", "23456");
                child2.put("value", 23456);

                var children = new GenericData.Array<GenericData.Record>(childrenSchema, List.of(child1, child2));
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", children);
                writer.append(record);

                children = new GenericData.Array<GenericData.Record>(childrenSchema, List.of());
                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", children);
                writer.append(record);
            }

            AvroRecordReader<CompositeMain> loader = new AvroRecordReader<>("/tmp/compositeListValue.avro", CompositeMain.class);
            Iterator<CompositeMain> it = loader.iterator();

            var expected = new CompositeMain("Apple",
                    List.of(new CompositeChild("12345", 12345), new CompositeChild("23456", 23456)));
            assertEquals(expected, it.next());
            assertEquals(new CompositeMain("Spotify", List.of()), it.next());
        }

        @Test
        void nullableCompositeListValue() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.avro")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("children").type().unionOf().nullType().and().type(childrenSchema).endUnion().noDefault()
                    .endRecord();

            try (DataFileWriter<GenericRecord> writer = writer("/tmp/compositeListValue.avro", schema)) {

                GenericData.Record child1 = new GenericData.Record(childSchema);
                child1.put("id", "12345");
                child1.put("value", 12345);

                GenericData.Record child2 = new GenericData.Record(childSchema);
                child2.put("id", "23456");
                child2.put("value", 23456);

                var children = new GenericData.Array<GenericData.Record>(childrenSchema, List.of(child1, child2));
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", children);
                writer.append(record);

                children = new GenericData.Array<GenericData.Record>(childrenSchema, List.of());
                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                writer.append(record);
            }

            AvroRecordReader<CompositeMain> loader = new AvroRecordReader<>("/tmp/compositeListValue.avro", CompositeMain.class);
            Iterator<CompositeMain> it = loader.iterator();

            var expected = new CompositeMain("Apple",
                    List.of(new CompositeChild("12345", 12345), new CompositeChild("23456", 23456)));
            assertEquals(expected, it.next());
        }
    }

    @Nested
    class SimpleTypeList {

        Schema ofType(Schema listSchema) {
            return SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.avro")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("children").type(listSchema).noDefault()
                    .endRecord();

        }

        public record WithIntegerList(String name, List<Integer> children) {
        }

        @Test
        void integerList() throws IOException {
            var schema = ofType(SchemaBuilder.builder().array().items().intType());

            try (DataFileWriter<GenericRecord> writer = writer("/tmp/withIntegerList.avro", schema)) {

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", List.of(1234, 5678));
                writer.append(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", List.of());
                writer.append(record);
            }

            AvroRecordReader<WithIntegerList> loader = new AvroRecordReader<>("/tmp/withIntegerList.avro", WithIntegerList.class);
            Iterator<WithIntegerList> it = loader.iterator();

            var expected = new WithIntegerList("Apple", List.of(1234, 5678));
            assertEquals(expected, it.next());
            assertEquals(new WithIntegerList("Spotify", List.of()), it.next());
        }

        public record WithStringList(String name, List<String> children) {
        }

        @Test
        void stringList() throws IOException {
            var schema = ofType(SchemaBuilder.builder().array().items().stringType());

            try (DataFileWriter<GenericRecord> writer = writer("/tmp/withStringList.avro", schema)) {

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", List.of("foo", "bar"));
                writer.append(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", List.of());
                writer.append(record);
            }

            AvroRecordReader<WithStringList> loader = new AvroRecordReader<>("/tmp/withStringList.avro", WithStringList.class);
            Iterator<WithStringList> it = loader.iterator();

            var expected = new WithStringList("Apple", List.of("foo", "bar"));
            assertEquals(expected, it.next());
            assertEquals(new WithStringList("Spotify", List.of()), it.next());
        }

        public enum OrgType {
            FOO, BAR, BAZ
        }

        public record WithEnumList(String name, List<OrgType> children) {
        }

        @Test
        void enumList() throws IOException {
            var schema = ofType(SchemaBuilder.builder().array().items().stringType());

            try (DataFileWriter<GenericRecord> writer = writer("/tmp/withEnumList.avro", schema)) {

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", List.of("FOO"));
                writer.append(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", List.of("FOO", "BAR", "BAZ"));
                writer.append(record);
            }

            AvroRecordReader<WithEnumList> loader = new AvroRecordReader<>("/tmp/withEnumList.avro", WithEnumList.class);
            Iterator<WithEnumList> it = loader.iterator();

            var expected = new WithEnumList("Apple", List.of(OrgType.FOO));
            assertEquals(expected, it.next());
            assertEquals(new WithEnumList("Spotify", List.of(OrgType.FOO, OrgType.BAR, OrgType.BAZ)), it.next());
        }

    }

    public record WithGenericField<T> (String name, T value) {
    }

    @Test
    void withGenericField() throws IOException {
        Schema schema = SchemaBuilder.builder()
                .record("PrimitivesAndObjects")
                .namespace("com.jerolba.avro")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("value").type().stringType().noDefault()
                .endRecord();

        try (DataFileWriter<GenericRecord> writer = writer("/tmp/withGenericField.avro", schema)) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("name", "foo");
            record.put("value", "bar");
            writer.append(record);
        }

        AvroRecordReader<WithGenericField> loader = new AvroRecordReader<>("/tmp/withGenericField.avro", WithGenericField.class);
        assertThrows(RuntimeException.class, () -> loader.iterator());
    }

    private DataFileWriter<GenericRecord> writer(String filePath, Schema schema) throws IOException {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        return dataFileWriter.create(schema, new File(filePath));
    }
}

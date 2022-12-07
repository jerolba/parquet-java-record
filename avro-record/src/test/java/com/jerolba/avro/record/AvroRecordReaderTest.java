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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AvroRecordReaderTest {

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

        var avroTest = new AvroTest("/tmp/privitiveObjects.avro");
        avroTest.write(schema, writer -> {
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
        });

        var it = avroTest.iterator(PrimitivesAndObjects.class);
        assertEquals(
                new PrimitivesAndObjects("foo", 1, 2, 3L, 4L, 5.0F, 6.0F, 7.0, 8.0, true, true),
                it.next());
        assertEquals(
                new PrimitivesAndObjects("bar", 10, null, 30L, null, 50.0F, null, 70.0, null, true, null),
                it.next());
        assertFalse(it.hasNext());
    }

    @Nested
    class EnumParsing {

        public enum OrgType {
            FOO, BAR, BAZ
        }

        public record WithEnum(String name, OrgType orgType) {
        }

        private final Schema schema = SchemaBuilder.builder()
                .record("WithEnum")
                .namespace("com.jerolba.avro")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("orgType").type().nullable().stringType().noDefault()
                .endRecord();

        @Test
        void withEnums() throws IOException {
            var avroTest = new AvroTest("/tmp/withEnum.avro");
            avroTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("orgType", OrgType.FOO.name());
                writer.append(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("orgType", null);
                writer.append(record);
            });

            Iterator<WithEnum> it = avroTest.iterator(WithEnum.class);
            assertEquals(new WithEnum("Apple", OrgType.FOO), it.next());
            assertEquals(new WithEnum("Spotify", null), it.next());
            assertFalse(it.hasNext());
        }

        @Test
        void unconvertibleEnums() throws IOException {
            var avroTest = new AvroTest("/tmp/withEnum.avro");
            avroTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("orgType", "invalid");
                writer.append(record);
            });

            Iterator<WithEnum> it = avroTest.iterator(WithEnum.class);
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

            var avroTest = new AvroTest("/tmp/compositeValue.avro");
            avroTest.write(schema, writer -> {
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
            });

            Iterator<CompositeMain> it = avroTest.iterator(CompositeMain.class);
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

            var avroTest = new AvroTest("/tmp/compositeValue.avro");
            avroTest.write(schema, writer -> {
                GenericData.Record child = new GenericData.Record(childSchema);
                child.put("id", "12345");
                child.put("value", 12345);

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("child", child);
                writer.append(record);
            });

            Iterator<CompositeMain> it = avroTest.iterator(CompositeMain.class);
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

            var avroTest = new AvroTest("/tmp/compositeValue.avro");
            avroTest.write(schema, writer -> {
                GenericData.Record child = new GenericData.Record(childSchema);
                child.put("id", "12345");
                child.put("value", 12345);

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("child", child);
                writer.append(record);
            });

            assertThrows(IllegalArgumentException.class, () -> avroTest.iterator(CompositeGeneric.class));
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

            var avroTest = new AvroTest("/tmp/compositeListValue.avro");
            avroTest.write(schema, writer -> {
                GenericData.Record child1 = new GenericData.Record(childSchema);
                child1.put("id", "12345");
                child1.put("value", 12345);

                GenericData.Record child2 = new GenericData.Record(childSchema);
                child2.put("id", "23456");
                child2.put("value", 23456);

                var children = new GenericData.Array<>(childrenSchema, List.of(child1, child2));
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", children);
                writer.append(record);

                children = new GenericData.Array<>(childrenSchema, List.of());
                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", children);
                writer.append(record);
            });

            Iterator<CompositeMain> it = avroTest.iterator(CompositeMain.class);

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

            var avroTest = new AvroTest("/tmp/compositeListValue.avro");
            avroTest.write(schema, writer -> {
                GenericData.Record child1 = new GenericData.Record(childSchema);
                child1.put("id", "12345");
                child1.put("value", 12345);

                GenericData.Record child2 = new GenericData.Record(childSchema);
                child2.put("id", "23456");
                child2.put("value", 23456);

                var children = new GenericData.Array<>(childrenSchema, List.of(child1, child2));
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", children);
                writer.append(record);

                children = new GenericData.Array<>(childrenSchema, List.of());
                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                writer.append(record);
            });

            Iterator<CompositeMain> it = avroTest.iterator(CompositeMain.class);

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

            var avroTest = new AvroTest("/tmp/withIntegerList.avro");
            avroTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", List.of(1234, 5678));
                writer.append(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", List.of());
                writer.append(record);
            });

            Iterator<WithIntegerList> it = avroTest.iterator(WithIntegerList.class);

            var expected = new WithIntegerList("Apple", List.of(1234, 5678));
            assertEquals(expected, it.next());
            assertEquals(new WithIntegerList("Spotify", List.of()), it.next());
        }

        public record WithStringList(String name, List<String> children) {
        }

        @Test
        void stringList() throws IOException {
            var schema = ofType(SchemaBuilder.builder().array().items().stringType());

            var avroTest = new AvroTest("/tmp/withStringList.avro");
            avroTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", List.of("foo", "bar"));
                writer.append(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", List.of());
                writer.append(record);
            });

            Iterator<WithStringList> it = avroTest.iterator(WithStringList.class);

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

            var avroTest = new AvroTest("/tmp/withEnumList.avro");
            avroTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", List.of("FOO"));
                writer.append(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", List.of("FOO", "BAR", "BAZ"));
                writer.append(record);
            });

            Iterator<WithEnumList> it = avroTest.iterator(WithEnumList.class);

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

        var avroTest = new AvroTest("/tmp/withGenericField.avro");
        avroTest.write(schema, writer -> {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("name", "foo");
            record.put("value", "bar");
            writer.append(record);
        });

        assertThrows(RuntimeException.class, () -> avroTest.iterator(WithGenericField.class));
    }

    @Nested
    class StreamTest {

        record Data(String id, int value) {

        }

        @BeforeEach
        public void setup() throws IOException {
            AvroRecordWriter<Data> writer = new AvroRecordWriter<>(Data.class);
            writer.write("/tmp/dataToRead.avro", List.of(
                    new Data("foo", 1),
                    new Data("bar", 2),
                    new Data("baz", 3)));
        }

        @Test
        void read() throws IOException {
            AvroRecordReader<Data> reader = new AvroRecordReader<>("/tmp/dataToRead.avro", Data.class);
            List<Data> readed = reader.stream().toList();
            assertEquals(new Data("foo", 1), readed.get(0));
            assertEquals(new Data("bar", 2), readed.get(1));
            assertEquals(new Data("baz", 3), readed.get(2));
        }

        @Test
        void skip() throws IOException {
            AvroRecordReader<Data> reader = new AvroRecordReader<>("/tmp/dataToRead.avro", Data.class);
            List<Data> readed = reader.stream().skip(2).toList();
            assertEquals(new Data("baz", 3), readed.get(0));
            assertEquals(1, readed.size());
        }

        @Test
        void limit() throws IOException {
            AvroRecordReader<Data> reader = new AvroRecordReader<>("/tmp/dataToRead.avro", Data.class);
            List<Data> readed = reader.stream().limit(1).toList();
            assertEquals(new Data("foo", 1), readed.get(0));
            assertEquals(1, readed.size());
        }

        @Test
        void empty() throws IOException {
            AvroRecordWriter<Data> writer = new AvroRecordWriter<>(Data.class);
            writer.write("/tmp/dataToRead.avro", List.of());
            AvroRecordReader<Data> reader = new AvroRecordReader<>("/tmp/dataToRead.avro", Data.class);
            List<Data> readed = reader.stream().toList();
            assertTrue(readed.isEmpty());
        }

        @Test
        void close() throws IOException {
            AvroRecordReader<Data> reader = new AvroRecordReader<>("/tmp/dataToRead.avro", Data.class);
            try (Stream<Data> stream = reader.stream()) {
                List<Data> readed = stream.toList();
                assertEquals(3, readed.size());
            }
        }

    }

    @Nested
    class ListTest {

        record Data(String id, int value) {

        }

        @BeforeEach
        public void setup() throws IOException {
            AvroRecordWriter<Data> writer = new AvroRecordWriter<>(Data.class);
            writer.write("/tmp/dataToRead.avro", List.of(
                    new Data("foo", 1),
                    new Data("bar", 2),
                    new Data("baz", 3)));
        }

        @Test
        void list() throws IOException {
            AvroRecordReader<Data> reader = new AvroRecordReader<>("/tmp/dataToRead.avro", Data.class);
            List<Data> readed = reader.toList();
            assertEquals(new Data("foo", 1), readed.get(0));
            assertEquals(new Data("bar", 2), readed.get(1));
            assertEquals(new Data("baz", 3), readed.get(2));
        }

        @Test
        void emptyList() throws IOException {
            AvroRecordWriter<Data> writer = new AvroRecordWriter<>(Data.class);
            writer.write("/tmp/dataToRead.avro", List.of());
            AvroRecordReader<Data> reader = new AvroRecordReader<>("/tmp/dataToRead.avro", Data.class);
            List<Data> readed = reader.toList();
            assertTrue(readed.isEmpty());
        }

    }

    private class AvroTest {

        private final String path;

        AvroTest(String path) {
            this.path = path;
        }

        public AvroTest write(Schema schema, WriterConsumer writerConsumer) throws IOException {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                var writer = dataFileWriter.create(schema, new File(path));
                writerConsumer.accept(writer);
            }
            return this;
        }

        public <T> AvroRecordReader<T> reader(Class<T> clazz) throws IOException {
            return new AvroRecordReader<>(path, clazz);
        }

        public <T> Iterator<T> iterator(Class<T> clazz) throws IOException {
            return reader(clazz).iterator();
        }
    }

    @FunctionalInterface
    public interface WriterConsumer {
        void accept(DataFileWriter<GenericRecord> writerConsumer) throws IOException;
    }
}

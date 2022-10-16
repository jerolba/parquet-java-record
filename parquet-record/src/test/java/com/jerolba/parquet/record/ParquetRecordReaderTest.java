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
package com.jerolba.parquet.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ParquetRecordReaderTest {

    @Test
    void primitivesAndObjectsTypes() throws IOException {
        Schema schema = SchemaBuilder.builder()
                .record("PrimitivesAndObjects")
                .namespace("com.jerolba.parquet")
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

        var parquetTest = new ParquetReaderTest("/tmp/privitiveObjects.parquet");
        parquetTest.write(schema, writer -> {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("name", "foo");
            record.put("intPrimitive", 1);
            record.put("intObject", 2);
            record.put("longPrimitive", 3L);
            record.put("longObject", 4L);
            record.put("floatPrimitive", 5.0);
            record.put("floatObject", 6.0);
            record.put("doublePrimitive", 7.0);
            record.put("doubleObject", 8.0);
            record.put("booleanPrimitive", true);
            record.put("booleanObject", true);
            writer.write(record);

            record = new GenericData.Record(schema);
            record.put("name", "bar");
            record.put("intPrimitive", 10);
            record.put("longPrimitive", 30L);
            record.put("floatPrimitive", 50.0);
            record.put("doublePrimitive", 70.0);
            record.put("booleanPrimitive", true);
            writer.write(record);
        });

        Iterator<PrimitivesAndObjects> it = parquetTest.iterator(PrimitivesAndObjects.class);
        assertEquals(new PrimitivesAndObjects("foo", 1, 2, 3L, 4L, 5.0F, 6.0F, 7.0, 8.0, true, true), it.next());
        assertEquals(new PrimitivesAndObjects("bar", 10, null, 30L, null, 50.0F, null, 70.0, null, true, null),
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
                .namespace("com.jerolba.parquet")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("orgType").type().nullable().stringType().noDefault()
                .endRecord();

        @Test
        void withEnums() throws IOException {
            var parquetTest = new ParquetReaderTest("/tmp/withEnum.parquet");
            parquetTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("orgType", OrgType.FOO.name());
                writer.write(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("orgType", null);
                writer.write(record);
            });

            Iterator<WithEnum> it = parquetTest.iterator(WithEnum.class);
            assertEquals(new WithEnum("Apple", OrgType.FOO), it.next());
            assertEquals(new WithEnum("Spotify", null), it.next());
            assertFalse(it.hasNext());
        }

        @Test
        void unconvertibleEnums() throws IOException {
            var parquetTest = new ParquetReaderTest("/tmp/withEnum.parquet");
            parquetTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("orgType", "invalid");
                writer.write(record);
            });

            Iterator<WithEnum> it = parquetTest.iterator(WithEnum.class);
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
                .namespace("com.jerolba.parquet")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("value").type().intType().noDefault()
                .endRecord();

        @Test
        void nullableCompositeValue() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.parquet")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("child").type().unionOf().nullType().and().type(childSchema).endUnion().noDefault()
                    .endRecord();

            var parquetTest = new ParquetReaderTest("/tmp/compositeValue.parquet");
            parquetTest.write(schema, writer -> {
                GenericData.Record child = new GenericData.Record(childSchema);
                child.put("id", "12345");
                child.put("value", 12345);

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("child", child);
                writer.write(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                writer.write(record);
            });

            Iterator<CompositeMain> it = parquetTest.iterator(CompositeMain.class);
            assertEquals(new CompositeMain("Apple", new CompositeChild("12345", 12345)), it.next());
            assertEquals(new CompositeMain("Spotify", null), it.next());
        }

        @Test
        void notNullableCompositeValue() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.parquet")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("child").type(childSchema).noDefault()
                    .endRecord();

            var parquetTest = new ParquetReaderTest("/tmp/compositeValue.parquet");
            parquetTest.write(schema, writer -> {
                GenericData.Record child = new GenericData.Record(childSchema);
                child.put("id", "12345");
                child.put("value", 12345);

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("child", child);
                writer.write(record);
            });

            Iterator<CompositeMain> it = parquetTest.iterator(CompositeMain.class);
            assertEquals(new CompositeMain("Apple", new CompositeChild("12345", 12345)), it.next());
        }

        @Test
        void genericCompositeNotSupported() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.parquet")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("child").type(childSchema).noDefault()
                    .endRecord();

            var parquetTest = new ParquetReaderTest("/tmp/compositeValue.parquet");
            parquetTest.write(schema, writer -> {
                GenericData.Record child = new GenericData.Record(childSchema);
                child.put("id", "12345");
                child.put("value", 12345);

                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("child", child);
                writer.write(record);
            });

            assertThrows(IllegalArgumentException.class, () -> parquetTest.iterator(CompositeGeneric.class));
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
                .namespace("com.jerolba.parquet")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("value").type().intType().noDefault()
                .endRecord();
        Schema childrenSchema = SchemaBuilder.builder().array().items(childSchema);

        @Test
        void nonNullableCompositeListValue() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.parquet")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("children").type(childrenSchema).noDefault()
                    .endRecord();

            var parquetTest = new ParquetReaderTest("/tmp/compositeListValue.parquet");
            parquetTest.write(schema, writer -> {
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
                writer.write(record);

                children = new GenericData.Array<>(childrenSchema, List.of());
                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", children);
                writer.write(record);
            });

            Iterator<CompositeMain> it = parquetTest.iterator(CompositeMain.class);
            var expected = new CompositeMain("Apple",
                    List.of(new CompositeChild("12345", 12345), new CompositeChild("23456", 23456)));
            assertEquals(expected, it.next());
            assertEquals(new CompositeMain("Spotify", List.of()), it.next());
        }

        @Test
        void nullableCompositeListValue() throws IOException {
            Schema schema = SchemaBuilder.builder()
                    .record("CompositeMain")
                    .namespace("com.jerolba.parquet")
                    .fields()
                    .name("name").type().stringType().noDefault()
                    .name("children").type().unionOf().nullType().and().type(childrenSchema).endUnion().noDefault()
                    .endRecord();

            var parquetTest = new ParquetReaderTest("/tmp/compositeListValue.parquet");
            parquetTest.write(schema, writer -> {
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
                writer.write(record);

                children = new GenericData.Array<>(childrenSchema, List.of());
                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                writer.write(record);
            });

            Iterator<CompositeMain> it = parquetTest.iterator(CompositeMain.class);
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
                    .namespace("com.jerolba.parquet")
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

            var parquetTest = new ParquetReaderTest("/tmp/withIntegerList.parquet");
            parquetTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", List.of(1234, 5678));
                writer.write(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", List.of());
                writer.write(record);
            });

            Iterator<WithIntegerList> it = parquetTest.iterator(WithIntegerList.class);

            var expected = new WithIntegerList("Apple", List.of(1234, 5678));
            assertEquals(expected, it.next());
            assertEquals(new WithIntegerList("Spotify", List.of()), it.next());
        }

        public record WithStringList(String name, List<String> children) {
        }

        @Test
        void stringList() throws IOException {
            var schema = ofType(SchemaBuilder.builder().array().items().stringType());

            var parquetTest = new ParquetReaderTest("/tmp/withStringList.parquet");
            parquetTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", List.of("foo", "bar"));
                writer.write(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", List.of());
                writer.write(record);
            });

            Iterator<WithStringList> it = parquetTest.iterator(WithStringList.class);

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

            var parquetTest = new ParquetReaderTest("/tmp/withEnumList.parquet");
            parquetTest.write(schema, writer -> {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "Apple");
                record.put("children", List.of("FOO"));
                writer.write(record);

                record = new GenericData.Record(schema);
                record.put("name", "Spotify");
                record.put("children", List.of("FOO", "BAR", "BAZ"));
                writer.write(record);
            });

            Iterator<WithEnumList> it = parquetTest.iterator(WithEnumList.class);

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
                .namespace("com.jerolba.parquet")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("value").type().stringType().noDefault()
                .endRecord();

        var parquetTest = new ParquetReaderTest("/tmp/withGenericField.parquet");

        parquetTest.write(schema, writer -> {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("name", "foo");
            record.put("value", "bar");
            writer.write(record);
        });
        assertThrows(RuntimeException.class, () -> parquetTest.iterator(WithGenericField.class));
    }

    private ParquetWriter<GenericData.Record> writer(String filePath, Schema schema) throws IOException {
        OutputFile outputFile = new OutputStreamOutputFile(new FileOutputStream(filePath));
        new FileSystemInputFile(new File(filePath));
        return AvroParquetWriter.<GenericData.Record>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withValidation(true)
                .withDictionaryEncoding(true)
                .withWriteMode(Mode.OVERWRITE)
                .build();
    }

    private class ParquetReaderTest {

        private final String path;

        ParquetReaderTest(String path) {
            this.path = path;
        }

        public ParquetReaderTest write(Schema schema, WriterConsumer writerConsumer) throws IOException {
            try (ParquetWriter<GenericData.Record> writer = writer(path, schema)) {
                writerConsumer.accept(writer);
            }
            return this;
        }

        public <T> ParquetRecordReader<T> reader(Class<T> clazz) throws IOException {
            return new ParquetRecordReader<>(path, clazz);
        }

        public <T> Iterator<T> iterator(Class<T> clazz) throws IOException {
            return reader(clazz).iterator();
        }

    }

    @FunctionalInterface
    public interface WriterConsumer {
        void accept(ParquetWriter<GenericData.Record> writerConsumer) throws IOException;
    }
}

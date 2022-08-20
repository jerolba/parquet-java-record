package com.jerolba.avro.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class RecordWriterTest {

    public enum OrgType {
        FOO, BAR, BAZ
    }

    public record PrimitivesAndObjects(String name,
            int intPrimitive, Integer intObject,
            long longPrimitive, Long longObject,
            float floatPrimitive, Float floatObject,
            double doublePrimitive, Double doubleObject,
            boolean booleanPrimitive, Boolean booleanObject) {
    }

    @Test
    void empryFile() throws IOException {
        var writer = new AvroRecordWriter<>(PrimitivesAndObjects.class);
        writer.write("/tmp/emptyFile.avro", List.of());

        var reader = new AvroRecordReader<>("/tmp/emptyFile.avro", PrimitivesAndObjects.class);
        var it = reader.iterator();
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, () -> it.next());
    }

    @Test
    void basicTypes() throws IOException {
        var rec = new PrimitivesAndObjects("foo", 1, 2, 3L, 4L, 5.0F, 6.0F, 7.0, 8.0, true, true);
        var writer = new AvroRecordWriter<>(PrimitivesAndObjects.class);
        writer.write("/tmp/primitivesAndObjectsWrite.avro", List.of(rec));

        var reader = new AvroRecordReader<>("/tmp/primitivesAndObjectsWrite.avro", PrimitivesAndObjects.class);
        PrimitivesAndObjects value = reader.iterator().next();
        assertEquals(rec, value);
    }

    @Test
    void basicTypesNulls() throws IOException {
        var rec = new PrimitivesAndObjects(null, 2, null, 4L, null, 6.0F, null, 8.0, null, true, null);
        var writer = new AvroRecordWriter<>(PrimitivesAndObjects.class);
        writer.write("/tmp/primitivesAndNullObjectsWrite.avro", List.of(rec));

        var reader = new AvroRecordReader<>("/tmp/primitivesAndNullObjectsWrite.avro", PrimitivesAndObjects.class);
        PrimitivesAndObjects value = reader.iterator().next();
        assertEquals(rec, value);
    }

    @Nested
    class EnumWriting {

        public record WithEnum(String name, OrgType orgType) {

        }

        @Test
        void withEnums() throws IOException {
            var rec = new WithEnum("Apple", OrgType.FOO);
            var writer = new AvroRecordWriter<>(WithEnum.class);
            writer.write("/tmp/withEnum.avro", List.of(rec));

            var reader = new AvroRecordReader<>("/tmp/withEnum.avro", WithEnum.class);
            WithEnum value = reader.iterator().next();
            assertEquals(rec, value);
        }

        @Test
        void withNullEnums() throws IOException {
            var rec = new WithEnum("Apple", null);
            var writer = new AvroRecordWriter<>(WithEnum.class);
            writer.write("/tmp/withNullEnums.avro", List.of(rec));

            var reader = new AvroRecordReader<>("/tmp/withNullEnums.avro", WithEnum.class);
            WithEnum value = reader.iterator().next();
            assertEquals(rec, value);
        }

    }

    @Nested
    class CompositedClasses {

        public record CompositeChild(String id, int value) {
        }

        public record CompositeMain(String name, CompositeChild child) {
        }

        @Test
        void compositeValue() throws IOException {
            CompositeChild child = new CompositeChild("Amount", 100);
            CompositeMain rec = new CompositeMain("Amazon", child);

            var writer = new AvroRecordWriter<>(CompositeMain.class);
            writer.write("/tmp/compositeMain.avro", List.of(rec));

            var reader = new AvroRecordReader<>("/tmp/compositeMain.avro", CompositeMain.class);
            CompositeMain value = reader.iterator().next();
            assertEquals(rec, value);
        }

        @Test
        void compositeValueWithNull() throws IOException {
            CompositeMain rec = new CompositeMain("Amazon", null);

            var writer = new AvroRecordWriter<>(CompositeMain.class);
            writer.write("/tmp/compositeMainWithNull.avro", List.of(rec));

            var reader = new AvroRecordReader<>("/tmp/compositeMainWithNull.avro", CompositeMain.class);
            CompositeMain value = reader.iterator().next();
            assertEquals(rec, value);
        }

        public record CompositeGeneric<T> (String name, T child) {
        }

        @Test
        void genericCompositeNotSupported() throws IOException {
            assertThrows(RuntimeException.class, () -> new AvroRecordWriter<>(CompositeGeneric.class));
        }

        public record RecursiveLoop(String id, RercursiveMain recursive) {
        }

        public record RecursiveChild(String id, RecursiveLoop child) {
        }

        public record RercursiveMain(String name, RecursiveChild child) {
        }

        @Test
        void recursiveCompositeNotSupported() throws IOException {
            assertThrows(RuntimeException.class, () -> new AvroRecordWriter<>(CompositeGeneric.class));
        }

    }

    @Nested
    class ChildCollections {

        public record CompositeChild(String id, int value) {
        }

        public record CompositeMain(String name, List<CompositeChild> child) {
        }

        @Test
        void withCollectionObject() throws IOException {
            CompositeChild child1 = new CompositeChild("Amount", 100);
            CompositeChild child2 = new CompositeChild("Size", 20);
            CompositeMain rec = new CompositeMain("Amazon", List.of(child1, child2));

            var writer = new AvroRecordWriter<>(CompositeMain.class);
            writer.write("/tmp/withCollectionObject.avro", List.of(rec));

            var reader = new AvroRecordReader<>("/tmp/withCollectionObject.avro", CompositeMain.class);
            CompositeMain value = reader.iterator().next();
            assertEquals(rec, value);
        }

        @Test
        void emptyCollectionObject() throws IOException {
            CompositeMain rec = new CompositeMain("Amazon", null);

            var writer = new AvroRecordWriter<>(CompositeMain.class);
            writer.write("/tmp/emptyCollectionObject.avro", List.of(rec));

            var reader = new AvroRecordReader<>("/tmp/emptyCollectionObject.avro", CompositeMain.class);
            CompositeMain value = reader.iterator().next();
            assertEquals(rec, value);
        }

        public record SimpleCollection(String name, List<Integer> sizes, List<String> names) {
        }

        @Test
        void withCollectionSimples() throws IOException {
            SimpleCollection rec = new SimpleCollection("Amazon", List.of(10, 20), List.of("FOO", "BAR"));

            var writer = new AvroRecordWriter<>(SimpleCollection.class);
            writer.write("/tmp/withCollectionSimples.avro", List.of(rec));

            var reader = new AvroRecordReader<>("/tmp/withCollectionSimples.avro", SimpleCollection.class);
            SimpleCollection value = reader.iterator().next();
            assertEquals(rec, value);
        }

        public record EnumCollection(String name, List<OrgType> orgTypes) {
        }

        @Test
        void withEnumCollection() throws IOException {
            EnumCollection rec = new EnumCollection("Amazon", List.of(OrgType.FOO, OrgType.BAR));

            var writer = new AvroRecordWriter<>(EnumCollection.class);
            writer.write("/tmp/withEnumCollection.avro", List.of(rec));

            var reader = new AvroRecordReader<>("/tmp/withEnumCollection.avro", EnumCollection.class);
            EnumCollection value = reader.iterator().next();
            assertEquals(rec, value);
        }
    }
}

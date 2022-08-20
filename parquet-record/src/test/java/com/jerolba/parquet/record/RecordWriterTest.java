package com.jerolba.parquet.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class RecordWriterTest {

    public record PrimitivesAndObjects(String name,
            int intPrimitive, Integer intObject,
            long longPrimitive, Long longObject,
            float floatPrimitive, Float floatObject,
            double doublePrimitive, Double doubleObject,
            boolean booleanPrimitive, Boolean booleanObject) {
    }

    @Test
    void empryFile() throws IOException {
        var writer = new ParquetRecordWriter<>(PrimitivesAndObjects.class);
        writer.write("/tmp/emptyFile.parquet", List.of());

        var reader = new ParquetRecordReader<>("/tmp/emptyFile.parquet", PrimitivesAndObjects.class);
        var it = reader.iterator();
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, () -> it.next());
    }

    @Test
    void basicTypes() throws IOException {
        var rec = new PrimitivesAndObjects("foo", 1, 2, 3L, 4L, 5.0F, 6.0F, 7.0, 8.0, true, true);
        var writer = new ParquetRecordWriter<>(PrimitivesAndObjects.class);
        writer.write("/tmp/primitivesAndObjectsWrite.parquet", List.of(rec));

        var reader = new ParquetRecordReader<>("/tmp/primitivesAndObjectsWrite.parquet", PrimitivesAndObjects.class);
        PrimitivesAndObjects value = reader.iterator().next();
        assertEquals(rec, value);
    }

    @Test
    void basicTypesNulls() throws IOException {
        var rec = new PrimitivesAndObjects(null, 2, null, 4L, null, 6.0F, null, 8.0, null, true, null);
        var writer = new ParquetRecordWriter<>(PrimitivesAndObjects.class);
        writer.write("/tmp/primitivesAndNullObjectsWrite.parquet", List.of(rec));

        var reader = new ParquetRecordReader<>("/tmp/primitivesAndNullObjectsWrite.parquet", PrimitivesAndObjects.class);
        PrimitivesAndObjects value = reader.iterator().next();
        assertEquals(rec, value);
    }

    @Nested
    class EnumWriting {

        public enum OrgType {
            FOO, BAR, BAZ
        }

        public record WithEnum(String name, OrgType orgType) {

        }

        @Test
        void withEnums() throws IOException {
            var rec = new WithEnum("Apple", OrgType.FOO);
            var writer = new ParquetRecordWriter<>(WithEnum.class);
            writer.write("/tmp/withEnum.parquet", List.of(rec));

            var reader = new ParquetRecordReader<>("/tmp/withEnum.parquet", WithEnum.class);
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

            var writer = new ParquetRecordWriter<>(CompositeMain.class);
            writer.write("/tmp/compositeMain.parquet", List.of(rec));

            var reader = new ParquetRecordReader<>("/tmp/compositeMain.parquet", CompositeMain.class);
            CompositeMain value = reader.iterator().next();
            assertEquals(rec, value);
        }

        @Test
        void compositeValueWithNull() throws IOException {
            CompositeMain rec = new CompositeMain("Amazon", null);

            var writer = new ParquetRecordWriter<>(CompositeMain.class);
            writer.write("/tmp/compositeMainWithNull.parquet", List.of(rec));

            var reader = new ParquetRecordReader<>("/tmp/compositeMainWithNull.parquet", CompositeMain.class);
            CompositeMain value = reader.iterator().next();
            assertEquals(rec, value);
        }

        public record CompositeGeneric<T> (String name, T child) {
        }

        @Test
        void genericCompositeNotSupported() throws IOException {
            assertThrows(RuntimeException.class, () -> new ParquetRecordWriter<>(CompositeGeneric.class));
        }

        public record RecursiveLoop(String id, RercursiveMain recursive) {
        }

        public record RecursiveChild(String id, RecursiveLoop child) {
        }

        public record RercursiveMain(String name, RecursiveChild child) {
        }

        @Test
        void recursiveCompositeNotSupported() throws IOException {
            assertThrows(RuntimeException.class, () -> new ParquetRecordWriter<>(CompositeGeneric.class));
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

            var writer = new ParquetRecordWriter<>(CompositeMain.class);
            writer.write("/tmp/withCollectionObject.parquet", List.of(rec));

            var reader = new ParquetRecordReader<>("/tmp/withCollectionObject.parquet", CompositeMain.class);
            CompositeMain value = reader.iterator().next();
            assertEquals(rec, value);
        }

        @Test
        void emptyCollectionObject() throws IOException {
            CompositeMain rec = new CompositeMain("Amazon", null);

            var writer = new ParquetRecordWriter<>(CompositeMain.class);
            writer.write("/tmp/emptyCollectionObject.parquet", List.of(rec));

            var reader = new ParquetRecordReader<>("/tmp/emptyCollectionObject.parquet", CompositeMain.class);
            CompositeMain value = reader.iterator().next();
            assertEquals(rec, value);
        }

        public record SimpleCollection(String name, List<Integer> sizes, List<String> names) {
        }

        @Test
        void withCollectionSimples() throws IOException {
            SimpleCollection rec = new SimpleCollection("Amazon", List.of(10, 20), List.of("FOO", "BAR"));

            var writer = new ParquetRecordWriter<>(SimpleCollection.class);
            writer.write("/tmp/withCollectionSimples.parquet", List.of(rec));

            var reader = new ParquetRecordReader<>("/tmp/withCollectionSimples.parquet", SimpleCollection.class);
            SimpleCollection value = reader.iterator().next();
            assertEquals(rec, value);
        }

    }
}

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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.record.annotation.Alias;

public class AvroRecordWriterTest {

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
        var writerTest = new AvroWriterTest<>("/tmp/emptyFile.avro", PrimitivesAndObjects.class);
        writerTest.write();

        var it = writerTest.getReadIterator();
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, () -> it.next());
    }

    @Test
    void basicTypes() throws IOException {
        var rec = new PrimitivesAndObjects("foo", 1, 2, 3L, 4L, 5.0F, 6.0F, 7.0, 8.0, true, true);
        var writerTest = new AvroWriterTest<>("/tmp/primitivesAndObjectsWrite.avro", PrimitivesAndObjects.class);
        writerTest.write(rec);

        PrimitivesAndObjects value = writerTest.getReadIterator().next();
        assertEquals(rec, value);
    }

    @Test
    void basicTypesNulls() throws IOException {
        var rec = new PrimitivesAndObjects(null, 2, null, 4L, null, 6.0F, null, 8.0, null, true, null);
        var writerTest = new AvroWriterTest<>("/tmp/primitivesAndNullObjectsWrite.avro", PrimitivesAndObjects.class);
        writerTest.write(rec);

        PrimitivesAndObjects value = writerTest.getReadIterator().next();
        assertEquals(rec, value);
    }

    @Nested
    class EnumWriting {

        public record WithEnum(String name, OrgType orgType) {

        }

        @Test
        void withEnums() throws IOException {
            var rec = new WithEnum("Apple", OrgType.FOO);
            var writerTest = new AvroWriterTest<>("/tmp/withEnum.avro", WithEnum.class);
            writerTest.write(rec);

            WithEnum value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

        @Test
        void withNullEnums() throws IOException {
            var rec = new WithEnum("Apple", null);
            var writerTest = new AvroWriterTest<>("/tmp/withNullEnums.avro", WithEnum.class);
            writerTest.write(rec);

            WithEnum value = writerTest.getReadIterator().next();
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

            var writerTest = new AvroWriterTest<>("/tmp/compositeMain.avro", CompositeMain.class);
            writerTest.write(rec);

            CompositeMain value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

        @Test
        void compositeValueWithNull() throws IOException {
            CompositeMain rec = new CompositeMain("Amazon", null);

            var writerTest = new AvroWriterTest<>("/tmp/compositeMainWithNull.avro", CompositeMain.class);
            writerTest.write(rec);

            CompositeMain value = writerTest.getReadIterator().next();
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

            var writerTest = new AvroWriterTest<>("/tmp/withCollectionObject.avro", CompositeMain.class);
            writerTest.write(rec);

            CompositeMain value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

        @Test
        void emptyCollectionObject() throws IOException {
            CompositeMain rec = new CompositeMain("Amazon", null);

            var writerTest = new AvroWriterTest<>("/tmp/emptyCollectionObject.avro", CompositeMain.class);
            writerTest.write(rec);

            CompositeMain value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

        public record SimpleCollection(String name, List<Integer> sizes, List<String> names) {
        }

        @Test
        void withCollectionSimples() throws IOException {
            SimpleCollection rec = new SimpleCollection("Amazon", List.of(10, 20), List.of("FOO", "BAR"));

            var writerTest = new AvroWriterTest<>("/tmp/withCollectionSimples.avro", SimpleCollection.class);
            writerTest.write(rec);

            SimpleCollection value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

        public record EnumCollection(String name, List<OrgType> orgTypes) {
        }

        @Test
        void withEnumCollection() throws IOException {
            EnumCollection rec = new EnumCollection("Amazon", List.of(OrgType.FOO, OrgType.BAR));

            var writerTest = new AvroWriterTest<>("/tmp/withEnumCollection.avro", EnumCollection.class);
            writerTest.write(rec);

            EnumCollection value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }
    }

    @Nested
    class FieldConversion {

        public record ByteShort(byte fromByte, short fromShort) {

        }

        @Test
        void writeByteShort() throws IOException {
            var rec = new ByteShort((byte) 1, (short) 2);

            var writerTest = new AvroWriterTest<>("/tmp/writeByteShort.avro", ByteShort.class);
            writerTest.write(rec);

            ByteShort value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

    }

    @Nested
    class AliasField {

        public record WithAlias(@Alias("frooo") String foo, @Alias("braaar") int bar) {

        }

        public record WithOutAlias(String frooo, int braaar) {

        }

        @Test
        void writeFileWithAlias() throws IOException {
            var rec = new WithAlias("some value", 1);
            var writerTest = new AvroWriterTest<>("/tmp/withAlias.avro", WithAlias.class);
            writerTest.write(rec);

            var reader = new AvroRecordReader<>("/tmp/withAlias.avro", WithOutAlias.class);
            WithOutAlias value = reader.iterator().next();
            assertEquals(rec.bar(), value.braaar());
            assertEquals(rec.foo(), value.frooo());
        }

        @Test
        void readFileWithAlias() throws IOException {
            var rec = new WithOutAlias("some value", 1);
            var writerTest = new AvroWriterTest<>("/tmp/withAlias.avro", WithOutAlias.class);
            writerTest.write(rec);

            var reader = new AvroRecordReader<>("/tmp/withAlias.avro", WithAlias.class);
            WithAlias value = reader.iterator().next();
            assertEquals(rec.braaar(), value.bar());
            assertEquals(rec.frooo(), value.foo());
        }

        @Test
        void writeAndReadWithAlias() throws IOException {
            var rec = new WithAlias("some value", 1);
            var writerTest = new AvroWriterTest<>("/tmp/withAlias.avro", WithAlias.class);
            writerTest.write(rec);

            WithAlias value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

    }

    private class AvroWriterTest<T> {

        private final String path;
        private final Class<T> type;

        AvroWriterTest(String path, Class<T> type) {
            this.path = path;
            this.type = type;
            new File(path).delete();
        }

        public void write(T... values) throws IOException {
            AvroRecordWriter<T> writer = new AvroRecordWriter<>(type);
            writer.write(path, List.of(values));
        }

        public Iterator<T> getReadIterator() throws IOException {
            var reader = new AvroRecordReader<>(path, type);
            return reader.iterator();
        }
    }
}

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
package com.jerolba.tarima;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.parquet.record.OutputStreamOutputFile;
import com.jerolba.parquet.record.ParquetRecordReader;

public class TarimaWriterTest {

    public record SimpleType(String name,
            int intPrimitive, Integer intObject, short a, Byte b) {
    }

    @Test
    void simpleType() throws IOException {
        var rec1 = new SimpleType("foo", 1, 2, (short) 4, (byte) 123);
        var rec2 = new SimpleType(null, 3, null, (short) 6, null);
        var writerTest = new ParquetWriterTest<>("/tmp/simpleType.parquet", SimpleType.class);
        writerTest.write(rec1, rec2);
        var it = writerTest.getReadIterator();
        System.out.println(it.next());
        System.out.println(it.next());
    }

    public record PrimitivesAndObjects(String name,
            int intPrimitive, Integer intObject,
            long longPrimitive, Long longObject,
            float floatPrimitive, Float floatObject,
            double doublePrimitive, Double doubleObject,
            boolean booleanPrimitive, Boolean booleanObject) {
    }

    @Test
    void emptyFile() throws IOException {
        var writerTest = new ParquetWriterTest<>("/tmp/emptyFile.parquet", PrimitivesAndObjects.class);
        writerTest.write();

        var it = writerTest.getReadIterator();
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, () -> it.next());
    }

    @Test
    void basicTypes() throws IOException {
        var rec = new PrimitivesAndObjects("foo", 1, 2, 3L, 4L, 5.0F, 6.0F, 7.0, 8.0, true, true);
        var writerTest = new ParquetWriterTest<>("/tmp/primitivesAndObjectsWrite.parquet", PrimitivesAndObjects.class);
        writerTest.write(rec);

        PrimitivesAndObjects value = writerTest.getReadIterator().next();
        assertEquals(rec, value);
    }

    @Test
    void basicTypesNulls() throws IOException {
        var rec = new PrimitivesAndObjects(null, 2, null, 4L, null, 6.0F, null, 8.0, null, true, null);
        var writerTest = new ParquetWriterTest<>("/tmp/primitivesAndNullObjectsWrite.parquet",
                PrimitivesAndObjects.class);
        writerTest.write(rec);

        PrimitivesAndObjects value = writerTest.getReadIterator().next();
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
            var writerTest = new ParquetWriterTest<>("/tmp/withEnum.parquet", WithEnum.class);
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

            var writerTest = new ParquetWriterTest<>("/tmp/compositeMain.parquet", CompositeMain.class);
            writerTest.write(rec);

            CompositeMain value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

        @Test
        void compositeValueWithNull() throws IOException {
            CompositeMain rec = new CompositeMain("Amazon", null);

            var writerTest = new ParquetWriterTest<>("/tmp/compositeMainWithNull.parquet", CompositeMain.class);
            writerTest.write(rec);

            CompositeMain value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

        public record CompositeGeneric<T>(String name, T child) {
        }

        @Test
        void genericCompositeNotSupported() throws IOException {
            OutputStreamOutputFile output = new OutputStreamOutputFile(
                    new FileOutputStream("/tmp/notsupported.parquet"));
            assertThrows(RuntimeException.class, () -> {
                try (var writer = TarimaWriter.builder(output, CompositeGeneric.class).build()) {
                }
            });
        }

        public record RecursiveLoop(String id, RercursiveMain recursive) {
        }

        public record RecursiveChild(String id, RecursiveLoop child) {
        }

        public record RercursiveMain(String name, RecursiveChild child) {
        }

        @Test
        void recursiveCompositeNotSupported() throws IOException {
            OutputStreamOutputFile output = new OutputStreamOutputFile(
                    new FileOutputStream("/tmp/notsupported.parquet"));
            assertThrows(RuntimeException.class, () -> {
                try (ParquetWriter<RecursiveLoop> writer = TarimaWriter.builder(output, RecursiveLoop.class)
                        .build()) {
                }
            });
        }

    }

    private class ParquetWriterTest<T> {

        private final String path;
        private final Class<T> type;

        ParquetWriterTest(String path, Class<T> type) {
            this.path = path;
            this.type = type;
            new File(path).delete();
        }

        public void write(T... values) throws IOException {
            OutputStreamOutputFile output = new OutputStreamOutputFile(new FileOutputStream(path));
            try (ParquetWriter<T> writer = TarimaWriter.builder(output, type).build()) {
                for (var v : values) {
                    writer.write(v);
                }
            }
        }

        public Iterator<T> getReadIterator() throws IOException {
            var reader = new ParquetRecordReader<>(path, type);
            return reader.iterator();
        }
    }
}

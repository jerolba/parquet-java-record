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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ParquettRecordWriterTest {

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

        public record CompositeGeneric<T> (String name, T child) {
        }

        @Test
        void genericCompositeNotSupported() throws IOException {
            var os = new FileOutputStream("/tmp/notsupported.parquet");
            var cfg = new ParquetRecordWriterConfig.Builder<>(os, CompositeGeneric.class).build();
            assertThrows(RuntimeException.class, () -> new ParquetRecordWriter<>(cfg));
        }

        public record RecursiveLoop(String id, RercursiveMain recursive) {
        }

        public record RecursiveChild(String id, RecursiveLoop child) {
        }

        public record RercursiveMain(String name, RecursiveChild child) {
        }

        @Test
        void recursiveCompositeNotSupported() throws IOException {
            var os = new FileOutputStream("/tmp/notsupported.parquet");
            var cfg = new ParquetRecordWriterConfig.Builder<>(os, CompositeGeneric.class).build();
            assertThrows(RuntimeException.class, () -> new ParquetRecordWriter<>(cfg));
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

            var writerTest = new ParquetWriterTest<>("/tmp/withCollectionObject.parquet", CompositeMain.class);
            writerTest.write(rec);

            CompositeMain value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

        @Test
        void emptyCollectionObject() throws IOException {
            CompositeMain rec = new CompositeMain("Amazon", null);

            var writerTest = new ParquetWriterTest<>("/tmp/emptyCollectionObject.parquet", CompositeMain.class);
            writerTest.write(rec);

            CompositeMain value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

        public record SimpleCollection(String name, List<Integer> sizes, List<String> names) {
        }

        @Test
        void withCollectionSimples() throws IOException {
            SimpleCollection rec = new SimpleCollection("Amazon", List.of(10, 20), List.of("FOO", "BAR"));

            var writerTest = new ParquetWriterTest<>("/tmp/withCollectionSimples.parquet", SimpleCollection.class);
            writerTest.write(rec);

            SimpleCollection value = writerTest.getReadIterator().next();
            assertEquals(rec, value);
        }

    }

    public enum Options {
        Opt1, Opt2, Opt3, Opt4;
    }

    public record VolumeComposed(String key, List<Integer> foo, List<Double> bar) {
    }

    public record VolumeChild(String id, String name, int value, Long distance, double metric, boolean active,
            VolumeComposed composed) {
    }

    public record VolumeMain(String id, String name, String foo, List<VolumeChild> child, int cont, Options option) {
    }

    @Test
    void highVolume() throws IOException {
        List<VolumeMain> data = new ArrayList<>();
        Options[] options = Options.values();
        Random rnd = new Random(1);
        for (int i = 0; i < 10_000; i++) {
            String mainId = RandomStringUtils.randomAlphanumeric(10);
            String mainName = RandomStringUtils.randomAscii(20);
            String foo = Integer.toString(rnd.nextInt(100000));

            List<VolumeChild> dataChild = new ArrayList<>();
            for (int j = 0; j < rnd.nextInt(10); j++) {
                String childId = RandomStringUtils.randomAlphanumeric(5);
                String childName = RandomStringUtils.randomAscii(5);
                int value = rnd.nextInt(10000000);
                Long distance = rnd.nextLong(100000);
                double metric = rnd.nextDouble();
                boolean active = rnd.nextBoolean();

                String key = RandomStringUtils.randomAscii(10);
                var fooInt = IntStream.range(0, Math.abs(rnd.nextInt(100)))
                        .mapToObj(k -> rnd.nextInt(k * k + 1))
                        .toList();
                var barDouble = IntStream.range(0, rnd.nextInt(100))
                        .mapToObj(k -> rnd.nextDouble(k * k + 1))
                        .toList();
                VolumeComposed composed = new VolumeComposed(key, fooInt, barDouble);
                dataChild.add(new VolumeChild(childId, childName, value, distance, metric, active, composed));
            }
            data.add(new VolumeMain(mainId, mainName, foo, dataChild, dataChild.size(), options[rnd.nextInt(4)]));
        }

        OutputStreamOutputFile output = new OutputStreamOutputFile(
                new FileOutputStream("/tmp/highVolume.parquet"));
        ParquetRecordWriterConfig<VolumeMain> config = new ParquetRecordWriterConfig.Builder<>(output, VolumeMain.class)
                .build();
        ParquetRecordWriter<VolumeMain> writer = new ParquetRecordWriter<>(config);
        writer.write(data);

        var reader = new ParquetRecordReader<>("/tmp/highVolume.parquet", VolumeMain.class);
        var it = reader.iterator();
        int i = 0;
        while (it.hasNext()) {
            assertEquals(data.get(i), it.next());
            i++;
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
            ParquetRecordWriterConfig<T> config = new ParquetRecordWriterConfig.Builder<>(output, type)
                    .build();
            ParquetRecordWriter<T> writer = new ParquetRecordWriter<>(config);
            writer.write(List.of(values));
        }

        public Iterator<T> getReadIterator() throws IOException {
            var reader = new ParquetRecordReader<>(path, type);
            return reader.iterator();
        }
    }
}

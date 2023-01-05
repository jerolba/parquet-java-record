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
package com.jerolba.carpet;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;

import com.jerolba.parquet.record.OutputStreamOutputFile;
import com.jerolba.parquet.record.ParquetRecordReader;

public class CarpetWriterCollectionOneLevelTest {

    @Test
    void simpleTypeCollection() throws IOException {

        record MainRecord(String name, List<Integer> ids, List<Double> amount) {
        }

        var rec1 = new MainRecord("foo", List.of(1, 2, 3), List.of(1.2, 3.2));
        var writerTest = new ParquetWriterTest<>("/tmp/simpleCollection.parquet", MainRecord.class);
        writerTest.write(rec1);
    }

    @Test
    void consecutiveNestedCollectionsAreNotSupported() {
        record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
        }

        var rec1 = new ConsecutiveNestedCollection("foo", List.of(List.of(1, 2, 3)));
        assertThrows(RecordTypeConversionException.class, () -> {
            var writerTest = new ParquetWriterTest<>("/tmp/consecutiveNestedCollectionsAreNotSupporteds.parquet",
                    ConsecutiveNestedCollection.class);
            writerTest.write(rec1);
        });
    }

    @Test
    void simpleCompositeCollection() throws IOException {

        record ChildRecord(String id, boolean active) {

        }
        record MainRecord(String name, List<ChildRecord> ids) {
        }

        var rec1 = new MainRecord("foo", List.of(new ChildRecord("Madrid", true), new ChildRecord("Sevilla", false)));
        var writerTest = new ParquetWriterTest<>("/tmp/simpleCompositeCollection.parquet", MainRecord.class);
        writerTest.write(rec1);
    }

    @Test
    void nonConsecutiveNestedCollections() throws IOException {
        record ChildCollection(String name, List<String> alias) {

        }
        record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
        }

        var rec1 = new NonConsecutiveNestedCollection("foo",
                List.of(new ChildCollection("Apple", List.of("MacOs", "OS X"))));
        var writerTest = new ParquetWriterTest<>("/tmp/nonConsecutiveNestedCollections.parquet",
                NonConsecutiveNestedCollection.class);
        writerTest.write(rec1);
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
            try (ParquetWriter<T> writer = CarpetWriter.builder(output, type)
                    .levelStructure(AnnotatedLevels.ONE)
                    .build()) {
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

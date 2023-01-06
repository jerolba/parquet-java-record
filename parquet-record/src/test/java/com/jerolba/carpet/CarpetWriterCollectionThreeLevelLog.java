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

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;

public class CarpetWriterCollectionThreeLevelLog {

    @Test
    void simpleTypeCollection() throws IOException {

        record MainRecord(String name, List<Integer> ids, List<Double> amount) {
        }

        var rec = new MainRecord("foo", List.of(1, 2, 3), List.of(1.2, 3.2));
        var writerTest = new ParquetWriterTest<>(MainRecord.class);
        writerTest.write(rec);
    }

    @Test
    void consecutiveNestedCollections() throws IOException {
        record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
        }

        var rec = new ConsecutiveNestedCollection("foo", List.of(List.of(1, 2, 3)));
        var writerTest = new ParquetWriterTest<>(ConsecutiveNestedCollection.class);
        writerTest.write(rec);
    }

    @Test
    void simpleCompositeCollection() throws IOException {

        record ChildRecord(String id, boolean active) {

        }
        record MainRecord(String name, List<ChildRecord> ids) {
        }

        var rec = new MainRecord("foo", List.of(new ChildRecord("Madrid", true), new ChildRecord("Sevilla", false)));
        var writerTest = new ParquetWriterTest<>(MainRecord.class);
        writerTest.write(rec);
    }

    @Test
    void consecutiveNestedCompositeCollection() throws IOException {

        record ChildRecord(String id, boolean active) {

        }
        record MainRecord(String name, List<List<ChildRecord>> ids) {
        }

        var rec = new MainRecord("foo",
                List.of(List.of(new ChildRecord("Madrid", true), new ChildRecord("Sevilla", false))));
        var writerTest = new ParquetWriterTest<>(MainRecord.class);
        writerTest.write(rec);

    }

    @Test
    void nonConsecutiveNestedCollections() throws IOException {
        record ChildCollection(String name, List<String> alias) {

        }
        record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
        }

        var rec = new NonConsecutiveNestedCollection("foo",
                List.of(new ChildCollection("Apple", List.of("MacOs", "OS X"))));
        var writerTest = new ParquetWriterTest<>(NonConsecutiveNestedCollection.class);
        writerTest.write(rec);
    }

    private class ParquetWriterTest<T> {

        private final Class<T> type;

        ParquetWriterTest(Class<T> type) {
            this.type = type;
        }

        public void write(T... values) throws IOException {
            try {
                CarpetRecordWriter writer = new CarpetRecordWriter(new TraceRecordConsumer(), type,
                        new CarpetConfiguration(AnnotatedLevels.THREE));
                for (T t : values) {
                    writer.write(t);
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

    }
}

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
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

public class CarpetWriterMapTest {

    @Test
    void simpleTypeMap() throws IOException {

        record SimpleTypeMap(String name, Map<String, Integer> ids, Map<String, Double> amount) {
        }

        var rec = new SimpleTypeMap("foo", Map.of("ABCD", 1, "EFGH", 2), Map.of("ABCD", 1.2, "EFGH", 2.3));
        var writerTest = new ParquetWriterTest<>("/tmp/simpleTypeMap.parquet", SimpleTypeMap.class);
        writerTest.write(rec);

        var reader = writerTest.getGenericRecordReader();
        GenericRecord record = reader.read();
        System.out.println(record);
    }

    @Test
    void consecutiveNestedMaps() throws IOException {
        record ConsecutiveNestedMaps(String id, Map<String, Map<String, Integer>> values) {
        }

        var rec = new ConsecutiveNestedMaps("foo", Map.of(
                "ABCD", Map.of("EFGH", 10, "IJKL", 20),
                "WXYZ", Map.of("EFGH", 30, "IJKL", 50)));
        var writerTest = new ParquetWriterTest<>("/tmp/consecutiveNestedMaps.parquet", ConsecutiveNestedMaps.class);
        writerTest.write(rec);

        var reader = writerTest.getGenericRecordReader();
        GenericRecord record = reader.read();
        System.out.println(record);
    }

    @Test
    void simpleMapCompositeValue() throws IOException {

        record ChildRecord(String id, boolean active) {

        }
        record MainRecord(String name, Map<String, ChildRecord> ids) {
        }

        var rec = new MainRecord("foo", Map.of(
                "MAD", new ChildRecord("Madrid", true),
                "BCN", new ChildRecord("Barcelona", true)));
        var writerTest = new ParquetWriterTest<>("/tmp/simpleMapCompositeValue.parquet", MainRecord.class);
        writerTest.write(rec);

        var reader = writerTest.getGenericRecordReader();
        GenericRecord record = reader.read();
        System.out.println(record);
    }

    @Test
    void consecutiveNestedCompositeMap() throws IOException {

        record ChildRecord(String id, boolean active) {

        }
        record MainRecord(String name, Map<String, Map<String, ChildRecord>> ids) {
        }

        var rec = new MainRecord("foo", Map.of(
                "10", Map.of("100", new ChildRecord("Madrid", true), "200", new ChildRecord("Sevilla", false)),
                "20", Map.of("200", new ChildRecord("Bilbao", false), "300", new ChildRecord("Zaragoza", false))));
        var writerTest = new ParquetWriterTest<>("/tmp/consecutiveNestedCompositeMap.parquet", MainRecord.class);
        writerTest.write(rec);

        var reader = writerTest.getGenericRecordReader();
        GenericRecord record = reader.read();
        System.out.println(record);
    }

    @Test
    void nonConsecutiveNestedMaps() throws IOException {
        record ChildCollection(String name, Map<String, Integer> alias) {

        }
        record nonConsecutiveNestedMaps(String id, Map<String, ChildCollection> values) {
        }

        var rec = new nonConsecutiveNestedMaps("foo", Map.of(
                "OS", new ChildCollection("Apple", Map.of("MacOs", 1000, "OS X", 2000)),
                "CP", new ChildCollection("MS", Map.of("Windows 10", 33, "Windows 11", 54))));
        var writerTest = new ParquetWriterTest<>("/tmp/nonConsecutiveNestedMaps.parquet",
                nonConsecutiveNestedMaps.class);
        writerTest.write(rec);

        var reader = writerTest.getGenericRecordReader();
        GenericRecord record = reader.read();
        System.out.println(record);
    }

    @Test
    void simpleMapCompositeKey() throws IOException {

        record ChildRecord(String id, boolean active) {

        }
        record MainRecord(String name, Map<ChildRecord, String> ids) {
        }

        var rec = new MainRecord("foo", Map.of(
                new ChildRecord("Madrid", true), "MAD",
                new ChildRecord("Barcelona", true), "BCN"));
        var writerTest = new ParquetWriterTest<>("/tmp/simpleMapCompositeKey.parquet", MainRecord.class);
        writerTest.write(rec);
    }

}

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
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class CarpetWriterMapTest {

    @Test
    void mapPrimitiveValue() throws IOException {

        record MapPrimitiveValue(String name, Map<String, Integer> ids, Map<String, Double> amount) {
        }

        var rec1 = new MapPrimitiveValue("foo", Map.of("ABCD", 1, "EFGH", 2), Map.of("ABCD", 1.2, "EFGH", 2.3));
        var rec2 = new MapPrimitiveValue("bar", Map.of("ABCD", 3, "EFGH", 4), Map.of("ABCD", 2.2, "EFGH", 3.3));
        var writerTest = new ParquetWriterTest<>(MapPrimitiveValue.class);
        writerTest.writeAndAssertReadIsEquals(rec1, rec2);
    }

    @Test
    void mapPrimitiveValueNull() throws IOException {

        record MapPrimitiveValueNull(String name, Map<String, Integer> ids, Map<String, Double> amount) {
        }

        var rec1 = new MapPrimitiveValueNull("foo", mapOf("ABCD", 1, "EFGH", 2), mapOf("ABCD", 1.2, "EFGH", 2.3));
        var rec2 = new MapPrimitiveValueNull("bar", mapOf("ABCD", null, "EFGH", 4), mapOf("ABCD", 2.2, "EFGH", null));
        var writerTest = new ParquetWriterTest<>(MapPrimitiveValueNull.class);
        writerTest.writeAndAssertReadIsEquals(rec1, rec2);
    }

    @Test
    void mapRecordValue() throws IOException {

        record ChildRecord(String id, boolean active) {

        }
        record MapRecordValue(String name, Map<String, ChildRecord> ids) {
        }

        var rec1 = new MapRecordValue("AVE", Map.of(
                "MAD", new ChildRecord("Madrid", true),
                "ZGZ", new ChildRecord("Zaragoza", false)));
        var rec2 = new MapRecordValue("AVE", Map.of(
                "MAD", new ChildRecord("Madrid", false),
                "CAT", new ChildRecord("Barcelona", true)));
        var writerTest = new ParquetWriterTest<>(MapRecordValue.class);
        writerTest.writeAndAssertReadIsEquals(rec1, rec2);
    }

    @Test
    void mapRecordValueNull() throws IOException {

        record ChildRecord(String id, boolean active) {

        }
        record MapRecordValueNull(String name, Map<String, ChildRecord> ids) {
        }

        var rec1 = new MapRecordValueNull("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                "ZGZ", new ChildRecord("Zaragoza", false)));
        var rec2 = new MapRecordValueNull("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                "BDJ", null));

        var writerTest = new ParquetWriterTest<>(MapRecordValueNull.class);
        writerTest.writeAndAssertReadIsEquals(rec1, rec2);
    }

    @Test
    void nestedMap_MapPrimitiveValue() throws IOException {

        record NestedMap_MapPrimitiveValue(String id, Map<String, Map<String, Integer>> values) {
        }

        var rec1 = new NestedMap_MapPrimitiveValue("Plane", Map.of(
                "ABCD", Map.of("EFGH", 10, "IJKL", 20),
                "WXYZ", Map.of("EFGH", 30, "IJKL", 50)));
        var rec2 = new NestedMap_MapPrimitiveValue("Boat", Map.of(
                "ABCD", Map.of("EFGH", 40, "IJKL", 50),
                "WXYZ", Map.of("EFGH", 70, "IJKL", 90)));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapPrimitiveValue.class);
        writerTest.writeAndAssertReadIsEquals(rec1, rec2);
    }

    @Test
    void nestedMap_MapPrimitiveValueNull() throws IOException {

        record NestedMap_MapPrimitiveValueNull(String id, Map<String, Map<String, Integer>> values) {
        }

        var rec1 = new NestedMap_MapPrimitiveValueNull("Plane", mapOf(
                "ABCD", mapOf("EFGH", 10, "IJKL", 20),
                "WXYZ", mapOf("EFGH", 30, "IJKL", 50)));
        var rec2 = new NestedMap_MapPrimitiveValueNull("Boat", mapOf(
                "ABCD", mapOf("EFGH", 40, "IJKL", null),
                "WXYZ", null));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapPrimitiveValueNull.class);
        writerTest.writeAndAssertReadIsEquals(rec1, rec2);
    }

    @Test
    void nestedMap_MapRecordValue() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record NestedMap_MapRecordValue(String name, Map<String, Map<String, ChildRecord>> ids) {
        }

        var rec1 = new NestedMap_MapRecordValue("Trip", Map.of(
                "AA", Map.of("FF", new ChildRecord("Madrid", true), "200", new ChildRecord("Sevilla", false)),
                "BB", Map.of("JJ", new ChildRecord("Bilbao", false), "300", new ChildRecord("Zaragoza", false))));
        var rec2 = new NestedMap_MapRecordValue("Hotel", Map.of(
                "ZZ", Map.of("100", new ChildRecord("Madrid", true), "200", new ChildRecord("Sevilla", false)),
                "YY", Map.of("200", new ChildRecord("Bilbao", false), "300", new ChildRecord("Zaragoza", false))));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapRecordValue.class);
        writerTest.writeAndAssertReadIsEquals(rec1, rec2);
    }

    @Test
    void nestedMap_MapRecordValueNull() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record NestedMap_MapRecordValueNull(String name, Map<String, Map<String, ChildRecord>> ids) {
        }

        var rec1 = new NestedMap_MapRecordValueNull("Trip", mapOf(
                "AA", mapOf("FF", new ChildRecord("Madrid", true), "200", new ChildRecord("Sevilla", false)),
                "BB", mapOf("JJ", new ChildRecord("Bilbao", false), "300", new ChildRecord("Zaragoza", false))));
        var rec2 = new NestedMap_MapRecordValueNull("Hotel", mapOf(
                "ZZ", mapOf("100", new ChildRecord("Madrid", true), "200", null),
                "YY", null));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapRecordValueNull.class);
        writerTest.writeAndAssertReadIsEquals(rec1, rec2);
    }

    @Test
    void nestedMap_Record_Map() throws IOException {

        record ChildWithMap(String name, Map<String, Integer> alias) {
        }
        record NestedMap_Record_Map(String id, Map<String, ChildWithMap> values) {
        }

        var rec = new NestedMap_Record_Map("foo", Map.of(
                "OS", new ChildWithMap("Apple", Map.of("MacOs", 1000, "OS X", 2000)),
                "CP", new ChildWithMap("MS", Map.of("Windows 10", 33, "Windows 11", 54))));
        var writerTest = new ParquetWriterTest<>(NestedMap_Record_Map.class);
        writerTest.writeAndAssertReadIsEquals(rec);
    }

    @Test
    void mapKeyRecord() throws IOException {

        record KeyRecord(String id, boolean active) {
        }
        record MapKeyRecord(String name, Map<KeyRecord, String> ids) {
        }

        var rec = new MapKeyRecord("foo", Map.of(
                new KeyRecord("Madrid", true), "MAD",
                new KeyRecord("Barcelona", true), "BCN"));
        var writerTest = new ParquetWriterTest<>(MapKeyRecord.class);
        writerTest.writeAndAssertReadIsEquals(rec);
    }

    @Test
    void mapKeyRecord_ValueRecord() throws IOException {

        record KeyRecord(String id, boolean active) {
        }
        record ValueRecord(String id, int age) {
        }
        record MapKeyRecord(String name, Map<KeyRecord, ValueRecord> ids) {
        }

        var rec = new MapKeyRecord("Time", Map.of(
                new KeyRecord("Madrid", true), new ValueRecord("MAD", 10),
                new KeyRecord("Barcelona", true), new ValueRecord("MAD", 10)));
        var writerTest = new ParquetWriterTest<>(MapKeyRecord.class);
        writerTest.writeAndAssertReadIsEquals(rec);
    }

    // Map.of doesn't support null values
    private <T, R> Map<T, R> mapOf(T key1, R value1, T key2, R value2) {
        HashMap<T, R> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }
}

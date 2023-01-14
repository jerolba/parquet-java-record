package com.jerolba.carpet;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class CarpetReaderTest {

    @Test
    void simpleType() throws IOException {

        record SimpleType(String name, int intPrimitive, Integer intObject, short a, Byte b) {
        }
        var rec1 = new SimpleType("foo", 1, 2, (short) 4, (byte) 123);
        var rec2 = new SimpleType(null, 3, null, (short) 6, null);
        var writerTest = new ParquetWriterTest<>("/tmp/simpleType.parquet", SimpleType.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void decimalTypes() throws IOException {

        record DecimalTypes(String name, double doublePrimitive, Float floatObject) {
        }
        var rec1 = new DecimalTypes("foo", 1.2, 3.5f);
        var rec2 = new DecimalTypes("bar", 6744.2, null);
        var writerTest = new ParquetWriterTest<>("/tmp/decimalTypes.parquet", DecimalTypes.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void booleanType() throws IOException {

        record BooleanTypes(String name, boolean booleanPrimitive, Boolean booleanObject) {
        }
        var rec1 = new BooleanTypes("foo", true, false);
        var rec2 = new BooleanTypes("bar", false, null);
        var writerTest = new ParquetWriterTest<>("/tmp/booleanTypes.parquet", BooleanTypes.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void enumType() throws IOException {
        enum Category {
            one, two, tree;
        }

        record EnumType(String name, Category category) {
        }
        var rec1 = new EnumType("foo", Category.one);
        var rec2 = new EnumType("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/enumType.parquet", EnumType.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedRecord() throws IOException {

        record Nested(String id, int value) {
        }

        record NestedRecord(String name, Nested nested) {
        }

        var rec1 = new NestedRecord("foo", new Nested("Madrid", 10));
        var rec2 = new NestedRecord("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedRecord.parquet", NestedRecord.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedCollectionPrimitive() throws IOException {

        record NestedCollectionPrimitive(String name, List<Integer> sizes) {
        }

        var rec1 = new NestedCollectionPrimitive("foo", List.of(1, 2, 3));
        var rec2 = new NestedCollectionPrimitive("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedCollectionPrimitive.parquet",
                NestedCollectionPrimitive.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedCollectionPrimitiveNullValues() throws IOException {

        record NestedCollectionPrimitive(String name, List<Integer> sizes) {
        }

        var rec1 = new NestedCollectionPrimitive("foo", asList(1, null, 3));
        var rec2 = new NestedCollectionPrimitive("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedCollectionPrimitive.parquet",
                NestedCollectionPrimitive.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedCollectionPrimitiveString() throws IOException {

        record NestedCollectionPrimitiveString(String name, List<String> sizes) {
        }

        var rec1 = new NestedCollectionPrimitiveString("foo", asList("1", null, "3"));
        var rec2 = new NestedCollectionPrimitiveString("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedCollectionPrimitiveString.parquet",
                NestedCollectionPrimitiveString.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedCollectionPrimitiveEnum() throws IOException {

        enum Category {
            one, two, three
        }

        record NestedCollectionPrimitiveEnum(String name, List<Category> category) {
        }

        var rec1 = new NestedCollectionPrimitiveEnum("foo", asList(Category.one, null, Category.three));
        var rec2 = new NestedCollectionPrimitiveEnum("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/NestedCollectionPrimitiveEnum.parquet",
                NestedCollectionPrimitiveEnum.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedCollectionComposite() throws IOException {

        record ChildItem(String id, boolean active) {

        }

        record NestedCollectionComposite(String name, List<ChildItem> status) {
        }

        var rec1 = new NestedCollectionComposite("foo",
                asList(new ChildItem("1", false), null, new ChildItem("2", true)));
        var rec2 = new NestedCollectionComposite("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedCollectionComposite.parquet",
                NestedCollectionComposite.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedCollectionMap() throws IOException {

        record NestedCollectionMap(String name, List<Map<String, Boolean>> status) {
        }

        var rec1 = new NestedCollectionMap("even",
                asList(Map.of("1", false, "2", true), null, Map.of("3", false, "4", true)));
        var rec2 = new NestedCollectionMap("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedCollectionMap.parquet",
                NestedCollectionMap.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedTwoCollectionPrimitive() throws IOException {

        record NestedTwoCollectionPrimitive(String name, List<List<Integer>> status) {
        }

        var rec1 = new NestedTwoCollectionPrimitive("foo", asList(asList(1, null, 3), null));
        var rec2 = new NestedTwoCollectionPrimitive("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/NestedTwoCollectionPrimitive.parquet",
                NestedTwoCollectionPrimitive.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedTwoCollectionComposite() throws IOException {

        record ChildItem(String id, boolean active) {
        }

        record NestedTwoCollectionComposite(String name, List<List<ChildItem>> status) {
        }

        var rec1 = new NestedTwoCollectionComposite("foo",
                asList(asList(new ChildItem("1", false), null, new ChildItem("2", true)), null));
        var rec2 = new NestedTwoCollectionComposite("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedTwoCollectionComposite.parquet",
                NestedTwoCollectionComposite.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedMapStringKeyPrimitiveValue() throws IOException {

        record NestedMapStringKeyPrimitiveValue(String name, Map<String, Integer> sizes) {
        }

        Map<String, Integer> map = new HashMap<>(Map.of("one", 1, "three", 3));
        map.put("two", null);
        var rec1 = new NestedMapStringKeyPrimitiveValue("foo", map);
        var rec2 = new NestedMapStringKeyPrimitiveValue("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedMapStringKeyPrimitiveValue.parquet",
                NestedMapStringKeyPrimitiveValue.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedMapStringKeyStringValue() throws IOException {

        record NestedMapStringKeyStringValue(String name, Map<String, String> sizes) {
        }

        Map<String, String> map = new HashMap<>(Map.of("one", "1", "three", "3"));
        map.put("two", null);
        var rec1 = new NestedMapStringKeyStringValue("foo", map);
        var rec2 = new NestedMapStringKeyStringValue("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedMapStringKeyPrimitiveKey.parquet",
                NestedMapStringKeyStringValue.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedMapPrimitiveKeyRecordValue() throws IOException {

        record ChildMap(String id, double value) {
        }
        record NestedMapPrimitiveKeyRecordValue(String name, Map<Integer, ChildMap> metrics) {
        }

        Map<Integer, ChildMap> map = new HashMap<>(Map.of(1, new ChildMap("Madrid", 12.0),
                3, new ChildMap("Bilbao", 23.0)));
        map.put(2, null);
        var rec1 = new NestedMapPrimitiveKeyRecordValue("foo", map);
        var rec2 = new NestedMapPrimitiveKeyRecordValue("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedMapPrimitiveKeyRecordValue.parquet",
                NestedMapPrimitiveKeyRecordValue.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedMapPrimitiveKeyListPrimitiveValue() throws IOException {

        record NestedMapPrimitiveKeyListPrimitiveValue(String name, Map<Short, List<Integer>> metrics) {
        }

        Map<Short, List<Integer>> map = new HashMap<>(Map.of((short) 1, List.of(1, 2, 3),
                (short) 3, List.of(4, 5, 6)));
        map.put((short) 2, null);
        var rec1 = new NestedMapPrimitiveKeyListPrimitiveValue("foo", map);
        var rec2 = new NestedMapPrimitiveKeyListPrimitiveValue("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedMapPrimitiveKeyListPrimitiveValue.parquet",
                NestedMapPrimitiveKeyListPrimitiveValue.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void nestedMapRecordKeyMapValue() throws IOException {

        enum Category {
            one, two, three;
        }

        record CompositeKey(String a, String b) {

        }

        record NestedMapRecordKeyMapValue(String name, Map<CompositeKey, Map<Category, String>> metrics) {
        }

        Map<CompositeKey, Map<Category, String>> map = new HashMap<>(Map.of(
                new CompositeKey("A", "B"), Map.of(Category.one, "ONE", Category.two, "TWO")));
        map.put(new CompositeKey("B", "C"), null);
        var rec1 = new NestedMapRecordKeyMapValue("foo", map);
        var rec2 = new NestedMapRecordKeyMapValue("bar", null);
        var writerTest = new ParquetWriterTest<>("/tmp/nestedMapRecordKeyMapValue.parquet",
                NestedMapRecordKeyMapValue.class);
        writerTest.write(rec1, rec2);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

    @Test
    void foooTypes() throws IOException {

        enum Category {
            one, two, three
        }

        record FooType(String name, int id, Category category) {
        }

        List<FooType> lst = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            lst.add(new FooType(i + "", 10, Category.one));
        }
        var writerTest = new ParquetWriterTest<>("/tmp/FooType.parquet", FooType.class);
        writerTest.write(lst);
        var reader = writerTest.getCarpetReader();
        System.out.println(reader.read());
        System.out.println(reader.read());
    }

}

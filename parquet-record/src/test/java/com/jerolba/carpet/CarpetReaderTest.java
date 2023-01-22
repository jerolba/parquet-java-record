package com.jerolba.carpet;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CarpetReaderTest {

    @Test
    void simpleType() throws IOException {

        record SimpleType(String name, int intPrimitive, Integer intObject, short a, Short b, byte c, Byte d) {
        }
        var rec1 = new SimpleType("foo", 1, 2, (short) 4, (short) 5, (byte) 123, (byte) 19);
        var rec2 = new SimpleType(null, 3, null, (short) 6, null, (byte) 7, null);
        var writerTest = new ParquetWriterTest<>(SimpleType.class);
        writerTest.write(rec1, rec2);

        var reader = writerTest.getCarpetReader();
        assertEquals(rec1, reader.read());
        assertEquals(rec2, reader.read());
    }

    @Test
    void decimalTypes() throws IOException {

        record DecimalTypes(String name, double doublePrimitive, Double doubleObject, float floatPrimitive,
                Float floatObject) {
        }
        var rec1 = new DecimalTypes("foo", 1.2, 2.4, 3.5f, 6.7f);
        var rec2 = new DecimalTypes("bar", 6744.2, null, 292.1f, null);
        var writerTest = new ParquetWriterTest<>(DecimalTypes.class);
        writerTest.write(rec1, rec2);

        var reader = writerTest.getCarpetReader();
        assertEquals(rec1, reader.read());
        assertEquals(rec2, reader.read());
    }

    @Test
    void booleanType() throws IOException {

        record BooleanTypes(String name, boolean booleanPrimitive, Boolean booleanObject) {
        }
        var rec1 = new BooleanTypes("foo", true, false);
        var rec2 = new BooleanTypes("bar", false, null);
        var writerTest = new ParquetWriterTest<>(BooleanTypes.class);
        writerTest.write(rec1, rec2);

        var reader = writerTest.getCarpetReader();
        assertEquals(rec1, reader.read());
        assertEquals(rec2, reader.read());
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
        var writerTest = new ParquetWriterTest<>(EnumType.class);
        writerTest.write(rec1, rec2);

        var reader = writerTest.getCarpetReader();
        assertEquals(rec1, reader.read());
        assertEquals(rec2, reader.read());
    }

    @Test
    void canProjectFields() throws IOException {

        record Original(String id, int value, boolean active, double amount, String category) {
        }

        var rec1 = new Original("foo", 1, true, 10.2, "SMALL");
        var rec2 = new Original("boo", 2, false, 22.3, "BIG");

        var writerTest = new ParquetWriterTest<>(Original.class);
        writerTest.write(rec1, rec2);

        record Projection(String id, int value, double amount) {
        }

        var reader = writerTest.getCarpetReader(Projection.class);

        var proj1 = new Projection("foo", 1, 10.2);
        var proj2 = new Projection("boo", 2, 22.3);

        assertEquals(proj1, reader.read());
        assertEquals(proj2, reader.read());

    }

    @Test
    void nestedRecord() throws IOException {

        record Nested(String id, int value) {
        }

        record NestedRecord(String name, Nested nested) {
        }

        var rec1 = new NestedRecord("foo", new Nested("Madrid", 10));
        var rec2 = new NestedRecord("bar", null);
        var writerTest = new ParquetWriterTest<>(NestedRecord.class);
        writerTest.write(rec1, rec2);

        var reader = writerTest.getCarpetReader();
        assertEquals(rec1, reader.read());
        assertEquals(rec2, reader.read());
    }

    @Test
    void projectNestedRecord() throws IOException {

        record Nested(String id, int value, double amount) {
        }

        record NestedRecord(String name, boolean active, Nested nested) {
        }

        var rec1 = new NestedRecord("foo", true, new Nested("Madrid", 10, 20.0));
        var rec2 = new NestedRecord("bar", false, null);
        var writerTest = new ParquetWriterTest<>(NestedRecord.class);
        writerTest.write(rec1, rec2);

        record ProjectedNested(String id, int value) {
        }

        record ProjectedNestedRecord(String name, ProjectedNested nested) {
        }

        var expectedRec1 = new ProjectedNestedRecord("foo", new ProjectedNested("Madrid", 10));
        var expectedRec2 = new ProjectedNestedRecord("bar", null);
        var reader = writerTest.getCarpetReader(ProjectedNestedRecord.class);
        assertEquals(expectedRec1, reader.read());
        assertEquals(expectedRec2, reader.read());
    }

    @Nested
    class ThreeLevelCollection {

        @Test
        void collectionPrimitive() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionFilteredByProjection() throws IOException {

            record CollectionPrimitive(String name, int size, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", 10, List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", 0, null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class);
            writerTest.write(rec1, rec2);

            record CollectionFiltered(String name, int size) {
            }

            var expectedRec1 = new CollectionFiltered("foo", 10);
            var expectedRec2 = new CollectionFiltered("bar", 0);

            var reader = writerTest.getCarpetReader(CollectionFiltered.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionPrimitiveNullValues() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", asList(1, null, 3));
            var rec2 = new CollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionPrimitiveString() throws IOException {

            record CollectionPrimitiveString(String name, List<String> sizes) {
            }

            var rec1 = new CollectionPrimitiveString("foo", asList("1", null, "3"));
            var rec2 = new CollectionPrimitiveString("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveString.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionPrimitiveEnum() throws IOException {

            enum Category {
                one, two, three
            }

            record CollectionPrimitiveEnum(String name, List<Category> category) {
            }

            var rec1 = new CollectionPrimitiveEnum("foo", asList(Category.one, null, Category.three));
            var rec2 = new CollectionPrimitiveEnum("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveEnum.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo",
                    asList(new ChildItem("1", false), null, new ChildItem("2", true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionCompositeProjected() throws IOException {

            record ChildItem(String id, int size, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo",
                    asList(new ChildItem("1", 10, false), null, new ChildItem("2", 20, true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class);
            writerTest.write(rec1, rec2);

            record ChildItemProjected(int size, String id) {
            }

            record CollectionProjected(String name, List<ChildItemProjected> status) {
            }

            var expectedRec1 = new CollectionProjected("foo",
                    asList(new ChildItemProjected(10, "1"),
                            null,
                            new ChildItemProjected(20, "2")));
            var expectedRec2 = new CollectionProjected("bar", null);

            var reader = writerTest.getCarpetReader(CollectionProjected.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionNestedMap() throws IOException {

            record CollectionNestedMap(String name, List<Map<String, Boolean>> status) {
            }

            var rec1 = new CollectionNestedMap("even",
                    asList(Map.of("1", false, "2", true),
                            null,
                            Map.of("3", false, "4", true)));
            var rec2 = new CollectionNestedMap("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedMap.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionNestedCollectionPrimitive() throws IOException {

            record CollectionNestedCollectionPrimitive(String name, List<List<Integer>> status) {
            }

            var rec1 = new CollectionNestedCollectionPrimitive("foo", asList(asList(1, null, 3), null));
            var rec2 = new CollectionNestedCollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionPrimitive.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionNestedCollectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionNestedCollectionComposite(String name, List<List<ChildItem>> status) {
            }

            var rec1 = new CollectionNestedCollectionComposite("foo",
                    asList(
                            asList(
                                    new ChildItem("1", false),
                                    null,
                                    new ChildItem("2", true)),
                            null));
            var rec2 = new CollectionNestedCollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionComposite.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }
    }

    @Nested
    class TwoLevelCollection {

        @Test
        void collectionPrimitive() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionFilteredByProjection() throws IOException {

            record CollectionPrimitive(String name, int size, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", 10, List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", 0, null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            record CollectionFiltered(String name, int size) {
            }

            var expectedRec1 = new CollectionFiltered("foo", 10);
            var expectedRec2 = new CollectionFiltered("bar", 0);

            var reader = writerTest.getCarpetReader(CollectionFiltered.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionPrimitiveNullValuesNotSupported() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", asList(1, null, 3));
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.TWO);
            assertThrows(NullPointerException.class, () -> writerTest.write(rec1));
        }

        @Test
        void collectionPrimitiveString() throws IOException {

            record CollectionPrimitiveString(String name, List<String> sizes) {
            }

            var rec1 = new CollectionPrimitiveString("foo", List.of("1", "2", "3"));
            var rec2 = new CollectionPrimitiveString("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveString.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionPrimitiveEnum() throws IOException {

            enum Category {
                one, two, three
            }

            record CollectionPrimitiveEnum(String name, List<Category> category) {
            }

            var rec1 = new CollectionPrimitiveEnum("foo", asList(Category.one, Category.two, Category.three));
            var rec2 = new CollectionPrimitiveEnum("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveEnum.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo", List.of(new ChildItem("1", false), new ChildItem("2", true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionCompositeProjected() throws IOException {

            record ChildItem(String id, int size, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo", List.of(
                    new ChildItem("1", 10, false),
                    new ChildItem("2", 20, true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            record ChildItemProjected(int size, String id) {
            }

            record CollectionProjected(String name, List<ChildItemProjected> status) {
            }

            var expectedRec1 = new CollectionProjected("foo",
                    List.of(new ChildItemProjected(10, "1"),
                            new ChildItemProjected(20, "2")));
            var expectedRec2 = new CollectionProjected("bar", null);

            var reader = writerTest.getCarpetReader(CollectionProjected.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionNestedMap() throws IOException {

            record CollectionNestedMap(String name, List<Map<String, Boolean>> status) {
            }

            var rec1 = new CollectionNestedMap("even", List.of(
                    Map.of("1", false, "2", true),
                    Map.of("3", false, "4", true)));
            var rec2 = new CollectionNestedMap("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedMap.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionNestedCollectionPrimitive() throws IOException {

            record CollectionNestedCollectionPrimitive(String name, List<List<Integer>> status) {
            }

            var rec1 = new CollectionNestedCollectionPrimitive("foo", List.of(List.of(1, 2, 3)));
            var rec2 = new CollectionNestedCollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionNestedCollectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionNestedCollectionComposite(String name, List<List<ChildItem>> status) {
            }

            var rec1 = new CollectionNestedCollectionComposite("foo",
                    List.of(List.of(new ChildItem("1", false), new ChildItem("2", true))));
            var rec2 = new CollectionNestedCollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionComposite.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }
    }

    @Test
    void nestedMapStringKeyPrimitiveValue() throws IOException {

        record NestedMapStringKeyPrimitiveValue(String name, Map<String, Integer> sizes) {
        }

        Map<String, Integer> map = new HashMap<>(Map.of("one", 1, "three", 3));
        map.put("two", null);
        var rec1 = new NestedMapStringKeyPrimitiveValue("foo", map);
        var rec2 = new NestedMapStringKeyPrimitiveValue("bar", null);
        var writerTest = new ParquetWriterTest<>(NestedMapStringKeyPrimitiveValue.class);
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
        var writerTest = new ParquetWriterTest<>(NestedMapStringKeyStringValue.class);
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
        var writerTest = new ParquetWriterTest<>(NestedMapPrimitiveKeyRecordValue.class);
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
        var writerTest = new ParquetWriterTest<>(NestedMapPrimitiveKeyListPrimitiveValue.class);
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
        var writerTest = new ParquetWriterTest<>(NestedMapRecordKeyMapValue.class);
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
        var writerTest = new ParquetWriterTest<>(FooType.class);
        writerTest.write(lst);
        var reader = writerTest.getCarpetReader();

        System.out.println(reader.read());
        System.out.println(reader.read());
    }

}

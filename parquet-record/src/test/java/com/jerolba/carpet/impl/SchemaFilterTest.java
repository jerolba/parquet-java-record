package com.jerolba.carpet.impl;

import static org.apache.parquet.schema.ConversionPatterns.listOfElements;
import static org.apache.parquet.schema.ConversionPatterns.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.SchemaFilter;
import com.jerolba.carpet.impl.read.SchemaValidation;
import com.jerolba.record.annotation.NotNull;

class SchemaFilterTest {

    SchemaValidation defaultReadConfig = new SchemaValidation(false, true);
    SchemaValidation nonStrictNumericConfig = new SchemaValidation(false, false);
    SchemaValidation supportMissingFields = new SchemaValidation(true, true);

    @Nested
    class FieldInt32Conversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveInteger(int value) {
            }
            assertEquals(groupType, filter.filter(PrimitiveInteger.class));

            record ObjectInteger(Integer value) {
            }
            assertEquals(groupType, filter.filter(ObjectInteger.class));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveInteger(int value) {
            }
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(PrimitiveInteger.class));

            record ObjectInteger(Integer value) {
            }
            assertEquals(groupType, filter.filter(ObjectInteger.class));
        }

        @Test
        void castToLongIsSupported() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveLong(long value) {
            }
            assertEquals(groupType, filter.filter(PrimitiveLong.class));

            record ObjectLong(Long value) {
            }
            assertEquals(groupType, filter.filter(ObjectLong.class));
        }

        @Test
        void castToShortIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveShort(short value) {
            }
            record ObjectShort(Short value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(PrimitiveShort.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(ObjectShort.class));

            SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
            assertEquals(groupType, filterNonStrict.filter(PrimitiveShort.class));
            assertEquals(groupType, filterNonStrict.filter(ObjectShort.class));
        }

        @Test
        void castToByteIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveByte(byte value) {
            }
            record ObjectByte(Byte value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(PrimitiveByte.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(ObjectByte.class));

            SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
            assertEquals(groupType, filterNonStrict.filter(PrimitiveByte.class));
            assertEquals(groupType, filterNonStrict.filter(ObjectByte.class));
        }

    }

    @Nested
    class FieldInt64Conversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveLong(long value) {
            }
            assertEquals(groupType, filter.filter(PrimitiveLong.class));

            record ObjectLong(Long value) {
            }
            assertEquals(groupType, filter.filter(ObjectLong.class));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveLong(long value) {
            }
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(PrimitiveLong.class));

            record ObjectLong(Long value) {
            }
            assertEquals(groupType, filter.filter(ObjectLong.class));
        }

        @Test
        void castToIntIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveInt(int value) {
            }
            record ObjectInteger(Integer value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(PrimitiveInt.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(ObjectInteger.class));

            SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
            assertEquals(groupType, filterNonStrict.filter(PrimitiveInt.class));
            assertEquals(groupType, filterNonStrict.filter(ObjectInteger.class));
        }

        @Test
        void castToShortIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveShort(short value) {
            }
            record ObjectShort(Short value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(PrimitiveShort.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(ObjectShort.class));

            SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
            assertEquals(groupType, filterNonStrict.filter(PrimitiveShort.class));
            assertEquals(groupType, filterNonStrict.filter(ObjectShort.class));
        }

        @Test
        void castToByteIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveByte(byte value) {
            }
            record ObjectByte(Byte value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(PrimitiveByte.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(ObjectByte.class));

            SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
            assertEquals(groupType, filterNonStrict.filter(PrimitiveByte.class));
            assertEquals(groupType, filterNonStrict.filter(ObjectByte.class));
        }

    }

    @Nested
    class FieldFloatConversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.FLOAT, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveFloat(float value) {
            }
            assertEquals(groupType, filter.filter(PrimitiveFloat.class));

            record ObjectFloat(Float value) {
            }
            assertEquals(groupType, filter.filter(ObjectFloat.class));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveFloat(float value) {
            }
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(PrimitiveFloat.class));

            record ObjectFloat(Float value) {
            }
            assertEquals(groupType, filter.filter(ObjectFloat.class));
        }

        @Test
        void castToDoubleIsSupported() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.FLOAT, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveDouble(double value) {
            }
            assertEquals(groupType, filter.filter(PrimitiveDouble.class));

            record ObjectDouble(Double value) {
            }
            assertEquals(groupType, filter.filter(ObjectDouble.class));
        }

    }

    @Nested
    class FieldDoubleConversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveDouble(double value) {
            }
            assertEquals(groupType, filter.filter(PrimitiveDouble.class));

            record ObjectDouble(Double value) {
            }
            assertEquals(groupType, filter.filter(ObjectDouble.class));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveDouble(double value) {
            }
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(PrimitiveDouble.class));

            record ObjectDouble(Double value) {
            }
            assertEquals(groupType, filter.filter(ObjectDouble.class));
        }

        @Test
        void castToFloatIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveFloat(float value) {
            }
            record ObjectFloat(Float value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(PrimitiveFloat.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.filter(ObjectFloat.class));

            SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
            assertEquals(groupType, filterNonStrict.filter(PrimitiveFloat.class));
            assertEquals(groupType, filterNonStrict.filter(ObjectFloat.class));
        }

    }

    @Nested
    class FieldStringConversion {

        @Test
        void fieldRequired() {
            Type field = Types.primitive(BINARY, Repetition.REQUIRED).as(stringType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record NotNullString(@NotNull String value) {
            }
            assertEquals(groupType, filter.filter(NotNullString.class));

            record NullableString(String value) {
            }
            assertEquals(groupType, filter.filter(NullableString.class));
        }

        @Test
        void fieldOptional() {
            Type field = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record NullableString(String value) {
            }
            assertEquals(groupType, filter.filter(NullableString.class));

            record NotNullString(@NotNull String value) {
            }
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(NotNullString.class));
        }

    }

    @Nested
    class FieldEnumConversion {

        enum Category {
            one, two
        }

        @Test
        void fieldRequired() {
            Type field = Types.primitive(BINARY, Repetition.REQUIRED).as(enumType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record NotNullEnum(@NotNull Category value) {
            }
            assertEquals(groupType, filter.filter(NotNullEnum.class));

            record NullableEnum(Category value) {
            }
            assertEquals(groupType, filter.filter(NullableEnum.class));
        }

        @Test
        void fieldOptional() {
            Type field = Types.primitive(BINARY, Repetition.OPTIONAL).as(enumType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record NullableEnum(Category value) {
            }
            assertEquals(groupType, filter.filter(NullableEnum.class));

            record NotNullEnum(@NotNull Category value) {
            }
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(NotNullEnum.class));
        }

        @Test
        void castToStringIsSupported() {
            Type field = Types.primitive(BINARY, Repetition.REQUIRED).as(enumType()).named("value");
            GroupType groupType = new MessageType("foo", field);

            record CastToString(String value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filterStrict.filter(CastToString.class));
        }

    }

    @Nested
    class ParquetFieldsMatchingRecordFields {

        @Test
        void fieldsMatch() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record AllPresent(String name, int age) {
            }
            assertEquals(groupType, filter.filter(AllPresent.class));
        }

        @Test
        void ifSchemaHasMoreFieldsThanNeededAreFilteredInProjection() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record OnlyName(String name) {
            }
            GroupType expectedName = new MessageType("foo", field1);
            assertEquals(expectedName, filter.filter(OnlyName.class));

            record OnlyAge(int age) {
            }
            GroupType expectedAge = new MessageType("foo", field2);
            assertEquals(expectedAge, filter.filter(OnlyAge.class));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededProjectionFails() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record MoreThanExisting(String name, int age, boolean active) {
            }

            assertThrows(RecordTypeConversionException.class, () -> filter.filter(MoreThanExisting.class));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededButSupportsItFieldsAreNulled() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);

            record MoreThanExisting(String name, int age, boolean active) {
            }

            SchemaFilter filter = new SchemaFilter(supportMissingFields, groupType);
            GroupType expectedAge = new MessageType("foo", field1, field2);
            assertEquals(expectedAge, filter.filter(MoreThanExisting.class));
        }

    }

    @Nested
    class Composite {

        @Test
        void compositeChild() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType childGroupType = new GroupType(Repetition.OPTIONAL, "child", childField1, childField2);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record Child(String id, int age) {
            }
            record CompositeMain(String name, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filter.filter(CompositeMain.class));
        }

        @Test
        void ifSchemaHasMoreFieldsThanNeededAreFilteredInProjection() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "age");
            Type childField3 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BOOLEAN, "active");
            GroupType childGroupType = new GroupType(Repetition.OPTIONAL, "child", childField1, childField2,
                    childField3);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record Child(String id, int age) {
            }
            record CompositeMain(String name, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            GroupType expectedChildGroupType = new GroupType(Repetition.OPTIONAL, "child", childField1, childField2);
            GroupType expectedGroup = new MessageType("foo", field1, expectedChildGroupType);
            assertEquals(expectedGroup, filter.filter(CompositeMain.class));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededProjectionFails() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "age");
            Type childField3 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BOOLEAN, "active");
            GroupType childGroupType = new GroupType(Repetition.OPTIONAL, "child", childField1, childField2,
                    childField3);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record Child(String id, int age, boolean active, double amount) {
            }
            record CompositeMain(String name, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(CompositeMain.class));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededButSupportsItFieldsAreNulled() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "age");
            Type childField3 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BOOLEAN, "active");
            GroupType childGroupType = new GroupType(Repetition.OPTIONAL, "child", childField1, childField2,
                    childField3);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record Child(String id, int age, boolean active, double amount) {
            }
            record CompositeMain(String name, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(supportMissingFields, groupType);

            assertEquals(groupType, filter.filter(CompositeMain.class));
        }
    }

    @Nested
    class SimpleClassesAreNotSupported {

        private static class NormalClass {
            private final String id;

            public NormalClass(String id) {
                this.id = id;
            }

            public String getId() {
                return id;
            }

        }

        @Test
        void javaBeanCanNotBeDeserialized() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("id");
            GroupType groupType = new MessageType("foo", field1);

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(NormalClass.class));
        }

        @Test
        void javaBeanCanNotBePartOfRecord() {
            Type field1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("id");
            Type childField1 = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("id");
            GroupType childGroupType = new GroupType(Repetition.OPTIONAL, "child", childField1);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record ParentClass(String id, NormalClass child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(ParentClass.class));

            record ChildRecord(String id) {
            }
            record ParentRecord(String id, ChildRecord child) {
            }
            assertEquals(groupType, filter.filter(ParentRecord.class));
        }

        @Test
        void bigIntegerIsNotSupported() {
            Type field1 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "id");
            GroupType groupType = new MessageType("foo", field1);

            record BigIntegerRecord(BigInteger id) {
            }
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(BigIntegerRecord.class));
        }

        @Test
        void bigDecimalIsNotSupported() {
            Type field1 = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field1);

            record BigDecimalRecord(BigDecimal value) {
            }
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.filter(BigDecimalRecord.class));
        }

    }

    @Nested
    class Collections {

        Type fieldId = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("id");
        Type fieldName = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("name");
        Type fieldAge = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "age");
        Type fieldActive = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BOOLEAN, "active");

        @Nested
        class OneLevelCollection {

            @Test
            void integerCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Integer> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void shortCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Short> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(LevelOnePrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
                assertEquals(groupType, filterNonStrict.filter(LevelOnePrimitive.class));
            }

            @Test
            void byteCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Byte> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(LevelOnePrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
                assertEquals(groupType, filterNonStrict.filter(LevelOnePrimitive.class));
            }

            @Test
            void longCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT64, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Long> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void floatCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.FLOAT, "values");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Float> values) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void floatFromDoubleCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.DOUBLE, "values");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Float> values) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(LevelOnePrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
                assertEquals(groupType, filterNonStrict.filter(LevelOnePrimitive.class));
            }

            @Test
            void doubleCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.DOUBLE, "values");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Double> values) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void stringCollection() {
                Type repeated = Types.primitive(BINARY, Repetition.REPEATED).as(stringType()).named("ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<String> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            enum Category {
                one, two
            }

            @Test
            void enumCollection() {
                Type repeated = Types.primitive(BINARY, Repetition.REPEATED).as(enumType()).named("categories");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Category> categories) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void enumToStringCollection() {
                Type repeated = Types.primitive(BINARY, Repetition.REPEATED).as(enumType()).named("categories");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<String> categories) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void compositeCollection() {
                GroupType repeated = new GroupType(Repetition.REPEATED, "child", fieldId, fieldAge);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOneComposite.class));
            }

            @Test
            void compositeCollectionMatchFields() {
                GroupType repeated = new GroupType(Repetition.REPEATED, "child", fieldId, fieldAge, fieldActive);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                GroupType repeatedExoected = new GroupType(Repetition.REPEATED, "child", fieldId, fieldAge);
                GroupType groupTypeExpected = new MessageType("foo", fieldName, repeatedExoected);
                assertEquals(groupTypeExpected, filter.filter(LevelOneComposite.class));
            }

            @Test
            void nestedCollectionsAreNotSupported() {
                GroupType repeatedChild = new GroupType(Repetition.REPEATED, "nested", fieldId, fieldAge);
                GroupType repeated = new GroupType(Repetition.REPEATED, "list", repeatedChild);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record Child(String id, int age) {
                }
                record NestedCollections(String name, List<List<Child>> list) {
                }

                assertThrows(RecordTypeConversionException.class, () -> filter.filter(NestedCollections.class));

                record Nested(List<Child> nested) {
                }
                record ValidLevelOneComposite(String name, List<Nested> list) {
                }
                assertEquals(groupType, filter.filter(ValidLevelOneComposite.class));
            }
        }

        @Nested
        class TwoLevelCollection {

            @Test
            void integerCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT32, "element");
                GroupType listType = listType(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Integer> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelTwoPrimitive.class));
            }

            @Test
            void shortCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT32, "element");
                GroupType listType = listType(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Short> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(LevelTwoPrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
                assertEquals(groupType, filterNonStrict.filter(LevelTwoPrimitive.class));
            }

            @Test
            void byteCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT32, "element");
                GroupType listType = listType(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Byte> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(LevelTwoPrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
                assertEquals(groupType, filterNonStrict.filter(LevelTwoPrimitive.class));
            }

            @Test
            void longCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT64, "element");
                GroupType listType = listType(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Long> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelTwoPrimitive.class));
            }

            @Test
            void floatCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.FLOAT, "element");
                GroupType listType = listType(Repetition.OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Float> values) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelTwoPrimitive.class));
            }

            @Test
            void floatFromDoubleCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.DOUBLE, "element");
                GroupType listType = listType(Repetition.OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Float> values) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(LevelTwoPrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
                assertEquals(groupType, filterNonStrict.filter(LevelTwoPrimitive.class));
            }

            @Test
            void doubleCollection() {
                Type repeated = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.DOUBLE, "element");
                GroupType listType = listType(Repetition.OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Double> values) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelTwoPrimitive.class));
            }

            @Test
            void stringCollection() {
                Type repeated = Types.primitive(BINARY, Repetition.REPEATED).as(stringType()).named("element");
                GroupType listType = listType(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelOnePrimitive(String name, List<String> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            enum Category {
                one, two
            }

            @Test
            void enumCollection() {
                Type repeated = Types.primitive(BINARY, Repetition.REPEATED).as(enumType()).named("element");
                GroupType listType = listType(Repetition.OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelOnePrimitive(String name, List<Category> categories) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void enumToStringCollection() {
                Type repeated = Types.primitive(BINARY, Repetition.REPEATED).as(enumType()).named("element");
                GroupType listType = listType(Repetition.OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelOnePrimitive(String name, List<String> categories) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void compositeCollection() {
                GroupType repeated = new GroupType(Repetition.REPEATED, "child", fieldId, fieldAge);
                GroupType listType = listType(Repetition.OPTIONAL, "child", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOneComposite.class));
            }

            @Test
            void compositeCollectionMatchFields() {
                GroupType repeated = new GroupType(Repetition.REPEATED, "child", fieldId, fieldAge, fieldActive);
                GroupType listType = listType(Repetition.OPTIONAL, "child", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                GroupType repeatedExpected = new GroupType(Repetition.REPEATED, "child", fieldId, fieldAge);
                GroupType listTypeExpected = listType(Repetition.OPTIONAL, "child", repeatedExpected);
                GroupType groupTypeExpected = new MessageType("foo", fieldName, listTypeExpected);
                assertEquals(groupTypeExpected, filter.filter(LevelOneComposite.class));
            }

            @Test
            void nestedCollectionsWithPrimitivesAreSupported() {
                Type repeatedChild = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT32, "element");
                GroupType listType = listType(Repetition.REPEATED, "element", repeatedChild);
                GroupType repeated = listType(Repetition.OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record NestedCollections(String name, List<List<Integer>> values) {
                }
                assertEquals(groupType, filter.filter(NestedCollections.class));

                record Nested(List<Integer> nested) {
                }
                record ValidLevelOneComposite(String name, List<Nested> list) {
                }
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(ValidLevelOneComposite.class));
            }

            @Test
            void nestedCollectionsWithGroupAreSupported() {
                GroupType repeatedChild = new GroupType(Repetition.REPEATED, "element", fieldId, fieldAge);
                GroupType listType = listType(Repetition.REPEATED, "element", repeatedChild);
                GroupType repeated = listType(Repetition.OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record Child(String id, int age) {
                }
                record NestedCollections(String name, List<List<Child>> values) {
                }
                assertEquals(groupType, filter.filter(NestedCollections.class));

                record Nested(List<Child> nested) {
                }
                record ValidLevelOneComposite(String name, List<Nested> list) {
                }
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(ValidLevelOneComposite.class));
            }
        }

        @Nested
        class ThreeLevelCollection {

            @Test
            void integerCollection() {
                Type repeated = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Integer> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelTwoPrimitive.class));
            }

            @Test
            void shortCollection() {
                Type repeated = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Short> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(LevelTwoPrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
                assertEquals(groupType, filterNonStrict.filter(LevelTwoPrimitive.class));
            }

            @Test
            void byteCollection() {
                Type repeated = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Byte> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(LevelTwoPrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
                assertEquals(groupType, filterNonStrict.filter(LevelTwoPrimitive.class));
            }

            @Test
            void longCollection() {
                Type repeated = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Long> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelTwoPrimitive.class));
            }

            @Test
            void floatCollection() {
                Type repeated = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Float> values) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelTwoPrimitive.class));
            }

            @Test
            void floatFromDoubleCollection() {
                Type repeated = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Float> values) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(LevelTwoPrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(nonStrictNumericConfig, groupType);
                assertEquals(groupType, filterNonStrict.filter(LevelTwoPrimitive.class));
            }

            @Test
            void doubleCollection() {
                Type repeated = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Double> values) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelTwoPrimitive.class));
            }

            @Test
            void stringCollection() {
                Type repeated = Types.primitive(BINARY, Repetition.OPTIONAL).as(stringType()).named("element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelOnePrimitive(String name, List<String> ids) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            enum Category {
                one, two
            }

            @Test
            void enumCollection() {
                Type repeated = Types.primitive(BINARY, Repetition.OPTIONAL).as(enumType()).named("element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelOnePrimitive(String name, List<Category> categories) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void enumToStringCollection() {
                Type repeated = Types.primitive(BINARY, Repetition.OPTIONAL).as(enumType()).named("element");
                GroupType listType = listOfElements(Repetition.OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelOnePrimitive(String name, List<String> categories) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOnePrimitive.class));
            }

            @Test
            void compositeCollection() {
                GroupType repeated = new GroupType(Repetition.OPTIONAL, "element", fieldId, fieldAge);
                GroupType listType = listOfElements(Repetition.OPTIONAL, "child", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.filter(LevelOneComposite.class));
            }

            @Test
            void compositeCollectionMatchFields() {
                GroupType repeated = new GroupType(Repetition.OPTIONAL, "element", fieldId, fieldAge, fieldActive);
                GroupType listType = listOfElements(Repetition.OPTIONAL, "child", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                GroupType repeatedExpected = new GroupType(Repetition.OPTIONAL, "element", fieldId, fieldAge);
                GroupType listTypeExpected = listOfElements(Repetition.OPTIONAL, "child", repeatedExpected);
                GroupType groupTypeExpected = new MessageType("foo", fieldName, listTypeExpected);
                assertEquals(groupTypeExpected, filter.filter(LevelOneComposite.class));
            }

            @Test
            void nestedCollectionsWithPrimitivesAreSupported() {
                Type repeatedChild = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "element");
                GroupType listType = listOfElements(Repetition.REPEATED, "element", repeatedChild);
                GroupType repeated = listOfElements(Repetition.OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record NestedCollections(String name, List<List<Integer>> values) {
                }
                assertEquals(groupType, filter.filter(NestedCollections.class));

                record Nested(List<Integer> nested) {
                }
                record ValidLevelOneComposite(String name, List<Nested> list) {
                }
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(ValidLevelOneComposite.class));
            }

            @Test
            void nestedCollectionsWithGroupAreSupported() {
                GroupType repeatedChild = new GroupType(Repetition.OPTIONAL, "element", fieldId, fieldAge);
                GroupType listType = listOfElements(Repetition.REPEATED, "element", repeatedChild);
                GroupType repeated = listOfElements(Repetition.OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record Child(String id, int age) {
                }
                record NestedCollections(String name, List<List<Child>> values) {
                }
                assertEquals(groupType, filter.filter(NestedCollections.class));

                record Nested(List<Child> nested) {
                }
                record ValidLevelOneComposite(String name, List<Nested> list) {
                }
                assertThrows(RecordTypeConversionException.class, () -> filter.filter(ValidLevelOneComposite.class));
            }
        }

    }
}

package com.jerolba.carpet.impl;

import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetReadConfiguration;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.SchemaFilter;
import com.jerolba.record.annotation.NotNull;

class SchemaFilterTest {

    CarpetReadConfiguration defaultReadConfig = new CarpetReadConfiguration(false, true);
    CarpetReadConfiguration nonStrictNumericConfig = new CarpetReadConfiguration(false, false);

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
    class FieldsMatching {

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

            CarpetReadConfiguration supportMissingFields = new CarpetReadConfiguration(true, true);
            SchemaFilter filter = new SchemaFilter(supportMissingFields, groupType);

            record MoreThanExisting(String name, int age, boolean active) {
            }

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

            CarpetReadConfiguration supportMissingFields = new CarpetReadConfiguration(true, true);
            SchemaFilter filter = new SchemaFilter(supportMissingFields, groupType);

            assertEquals(groupType, filter.filter(CompositeMain.class));
        }
    }
}

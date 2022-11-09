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
package com.jerolba.avro.record;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.RecordComponent;

import org.junit.jupiter.api.Test;

class NotNullFieldTest {

    public record UsingJavaxNonNunll(String name, @javax.annotation.Nonnull String value) {

    }

    @Test
    void usingJavaxNotNull() {
        RecordComponent[] fields = UsingJavaxNonNunll.class.getRecordComponents();
        assertFalse(NotNullField.isNotNull(fields[0]));
        assertTrue(NotNullField.isNotNull(fields[1]));
    }

    public record UsingRecordNonNunll(String name, @com.jerolba.record.annotation.NotNull String value) {

    }

    @Test
    void usingRecordNotNull() {
        RecordComponent[] fields = UsingRecordNonNunll.class.getRecordComponents();
        assertFalse(NotNullField.isNotNull(fields[0]));
        assertTrue(NotNullField.isNotNull(fields[1]));
    }

}

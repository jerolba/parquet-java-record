package com.jerolba.carpet;

import java.lang.reflect.RecordComponent;

record RecordField(Class<?> targetClass, RecordComponent recordComponent, String fieldName, int idx) {

}

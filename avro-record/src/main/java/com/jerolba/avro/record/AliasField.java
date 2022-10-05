package com.jerolba.avro.record;

import java.lang.reflect.RecordComponent;

import com.jerolba.record.Alias;

class AliasField {

    static String getFieldName(RecordComponent recordComponent) {
        Alias annotation = recordComponent.getAnnotation(Alias.class);
        if (annotation == null) {
            return recordComponent.getName();
        }
        return annotation.value();
    }

}

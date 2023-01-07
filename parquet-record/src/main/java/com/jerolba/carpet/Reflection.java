package com.jerolba.carpet;

import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.util.function.Function;

class Reflection {

    static Function<Object, Object> recordAccessor(Class<?> targetClass, RecordComponent recordComponent)
            throws Throwable {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodHandle findVirtual = lookup.findVirtual(targetClass, recordComponent.getName(),
                methodType(recordComponent.getType()));
        CallSite site = LambdaMetafactory.metafactory(lookup,
                "apply",
                methodType(Function.class),
                methodType(Object.class, Object.class),
                findVirtual,
                methodType(recordComponent.getType(), targetClass));
        return (Function<Object, Object>) site.getTarget().invokeExact();
    }

}

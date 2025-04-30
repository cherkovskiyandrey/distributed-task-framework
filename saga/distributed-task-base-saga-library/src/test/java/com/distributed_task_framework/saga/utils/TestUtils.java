package com.distributed_task_framework.saga.utils;

import com.distributed_task_framework.saga.exceptions.MethodNotFoundException;
import lombok.experimental.UtilityClass;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

@UtilityClass
public class TestUtils {

    public Method findMethod(Class<?> cls, String name, Class<?> returnType, Collection<Class<?>> parameters) {
        return ReflectionHelper.allMethods(cls)
            .filter(m -> Objects.equals(m.getName(), name)
                && Objects.equals(m.getReturnType(), returnType)
                && Arrays.equals(m.getParameterTypes(), parameters.toArray())
            )
            .findFirst()
            .orElseThrow(() -> new MethodNotFoundException(name));
    }
}

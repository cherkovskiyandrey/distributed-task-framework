package com.distributed_task_framework.saga.utils;

import jakarta.annotation.Nullable;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
@UtilityClass
public class LambdaHelper {

    //todo: use cache
    public void printName(@Nullable Object lambda) {
        if (lambda == null) {
            return;
        }
        for (Class<?> cl = lambda.getClass(); cl != null; cl = cl.getSuperclass()) {
            try {
                Method m = cl.getDeclaredMethod("writeReplace");
                m.setAccessible(true);
                Object replacement = m.invoke(lambda);
                if(!(replacement instanceof SerializedLambda)) {
                    break;// custom interface implementation
                }
                SerializedLambda l = (SerializedLambda) replacement;
                var name = l.getImplClass() + "::" + l.getImplMethodSignature();
                log.info("========================>>>>>> {} => {}", lambda, name);
                return;
            } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        log.error("========================>>>>>> {} => unknown name", lambda);
    }
}

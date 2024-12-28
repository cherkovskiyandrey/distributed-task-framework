package com.distributed_task_framework.saga;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BiConsumer;

public class TestLambda {

    interface DebuggableBiConsumer<A, B> extends BiConsumer<A, B>, Serializable {}

    public static void testStaticMethod(String s, Integer i) {
        System.out.println("test method is invoked");
    }

    @Test
    void test() {
        DebuggableBiConsumer<String, Integer> lambda = (s, i) -> System.out.println(s + i);
        //printName(lambda);
        printName(TestLambda::testStaticMethod);
    }

    private void printName(DebuggableBiConsumer<String, Integer> lambda) {
        for (Class<?> cl = lambda.getClass(); cl != null; cl = cl.getSuperclass()) {
            try {
                Method m = cl.getDeclaredMethod("writeReplace");
                m.setAccessible(true);
                Object replacement = m.invoke(lambda);
                if(!(replacement instanceof SerializedLambda)) {
                    break;// custom interface implementation
                }
                SerializedLambda l = (SerializedLambda) replacement;
                System.out.println(l.getImplClass() + "::" + l.getImplMethodName());
                return;
            } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("unknown property");
    }
}

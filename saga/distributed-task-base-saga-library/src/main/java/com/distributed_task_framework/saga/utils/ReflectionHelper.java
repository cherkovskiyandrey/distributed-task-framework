package com.distributed_task_framework.saga.utils;

import lombok.experimental.UtilityClass;
import org.springframework.lang.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@UtilityClass
public class ReflectionHelper {

    public static Stream<Method> allMethods(Class<?> cls) {

        class SpliteratorMethods implements Spliterator<Method> {
            private Class<?> currentClass;
            private int methodIdx;

            public SpliteratorMethods(Class<?> currentClass) {
                this.currentClass = Objects.requireNonNull(currentClass);
                this.methodIdx = 0;
            }

            @Override
            public boolean tryAdvance(Consumer<? super Method> action) {
                if (currentClass == null) {
                    return false;
                }
                if (currentClass.getDeclaredMethods().length <= methodIdx) {
                    currentClass = currentClass.getSuperclass();
                    methodIdx = 0;
                }
                if (currentClass == null) {
                    return false;
                }

                action.accept(currentClass.getDeclaredMethods()[methodIdx++]);
                return true;
            }

            @Override
            public Spliterator<Method> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                return Long.MAX_VALUE;
            }

            @Override
            public int characteristics() {
                return ORDERED | NONNULL | IMMUTABLE;
            }
        }

        return StreamSupport.stream(new SpliteratorMethods(cls), false);
    }


    /**
     * The copy code from spring: org.springframework.util.ReflectionUtils#invokeMethod(java.lang.reflect.Method, java.lang.Object, java.lang.Object...)
     *
     * @param method
     * @param target
     * @param args
     * @return
     */
    @Nullable
    public static Object invokeMethod(Method method, @Nullable Object target, @Nullable Object... args) {
        try {
            return method.invoke(target, args);
        } catch (Exception ex) {
            handleReflectionException(ex);
        }
        throw new IllegalStateException("Should never get here");
    }

    public static void handleReflectionException(Throwable ex) {
        if (ex instanceof NoSuchMethodException) {
            throw new IllegalStateException("Method not found: " + ex.getMessage());
        }
        if (ex instanceof IllegalAccessException) {
            throw new IllegalStateException("Could not access method or field: " + ex.getMessage());
        }
        if (ex instanceof InvocationTargetException invocationTargetException) {
            handleReflectionException(invocationTargetException.getTargetException());
        }
        if (ex instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (ex instanceof Error error) {
            throw error;
        }
        throw new UndeclaredThrowableException(ex);
    }
}

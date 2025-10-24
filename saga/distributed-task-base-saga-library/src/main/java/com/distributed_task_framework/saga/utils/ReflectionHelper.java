package com.distributed_task_framework.saga.utils;

import com.google.common.collect.Sets;
import lombok.experimental.UtilityClass;
import org.springframework.lang.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@UtilityClass
public class ReflectionHelper {

    /**
     * Find all methods in the hierarchy of provided class for provided overridden method.
     *
     * @param method      overridden method in the last class of hierarchy
     * @param targetClass the last class in the hierarchy to begin searching from
     * @return
     */
    public List<Method> findAllMethodsForLastOverride(Method method, Class<?> targetClass) {
        return allMethods(targetClass).filter(m -> isMethodOverrideFor(method, m)).toList();
    }

    private boolean isMethodOverrideFor(Method overrideMethod, Method method) {
        return Objects.equals(overrideMethod.getName(), method.getName())
            && Arrays.equals(overrideMethod.getParameterTypes(), method.getParameterTypes())
            && (
            Objects.equals(overrideMethod.getReturnType(), method.getReturnType())
                //check covariant types
                || method.getReturnType().isAssignableFrom(overrideMethod.getReturnType())
        );
    }

    /**
     * Return stream of all methods (included abstract, methods in the interfaces) in the hierarchy.
     * Walk direction is from current class up to hierarchy.
     * In scope of certain class for ordering is undefined.
     *
     * @param cls
     * @return
     */
    public static Stream<Method> allMethods(Class<?> cls) {

        class SpliteratorMethods implements Spliterator<Method> {
            private final Queue<Class<?>> currentInterfaces;
            private final Set<Class<?>> visitedClasses;
            private Class<?> currentClass;
            private Class<?> currentInterface;
            private Method[] currentMethods;
            private int currentIdx;

            public SpliteratorMethods(Class<?> currentClass) {
                Objects.requireNonNull(currentClass);
                this.visitedClasses = Sets.newHashSet();
                this.currentInterfaces = new ArrayDeque<>();
                this.currentClass = currentClass;
                this.currentMethods = currentClass.getDeclaredMethods();
            }

            @Override
            public boolean tryAdvance(Consumer<? super Method> action) {
                Class<?> currentType = null;
                int idx = -1;

                do {
                    if (currentInterface == null && !currentInterfaces.isEmpty()) {
                        currentInterface = currentInterfaces.poll();
                        currentMethods = currentInterface.getDeclaredMethods();
                        currentInterfaces.addAll(filterOnlyNew(currentInterface.getInterfaces()));

                    } else if (currentInterface == null && currentMethods == null && currentClass != null) {
                        currentMethods = currentClass.getDeclaredMethods();

                    } else if (currentInterface != null) {
                        if (currentMethods.length <= currentIdx) {
                            currentIdx = 0;
                            currentInterface = null;
                            currentMethods = null;
                            continue;
                        }
                        currentType = currentInterface;
                        idx = currentIdx++;
                        break;

                    } else if (currentClass != null) {
                        if (currentMethods.length <= currentIdx) {
                            currentIdx = 0;
                            currentInterfaces.addAll(filterOnlyNew(currentClass.getInterfaces()));
                            currentClass = currentClass.getSuperclass() != Object.class ? currentClass.getSuperclass() : null;
                            currentMethods = currentClass != null ? currentClass.getDeclaredMethods() : null;
                            continue;
                        }
                        currentType = currentClass;
                        idx = currentIdx++;
                        break;

                    } else {
                        break;
                    }
                } while (true);

                if (currentType == null) {
                    return false;
                }

                action.accept(currentMethods[idx]);
                return true;
            }

            private Collection<Class<?>> filterOnlyNew(Class<?>[] interfaces) {
                var newInterfaces = Sets.newHashSet(Sets.difference(Sets.newHashSet(interfaces), visitedClasses));
                visitedClasses.addAll(newInterfaces);
                return newInterfaces;
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

package com.distributed_task_framework.saga.utils;

import com.distributed_task_framework.saga.models.SagaMethod;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReflectionHelperTest {

    interface ReturnType {
    }

    interface ExtendedReturnType extends ReturnType {
    }

    interface ArgumentType {
    }

    interface AI {
        ReturnType foo();
    }

    interface BI extends AI {
        default ReturnType foo(ArgumentType arg) {
            return null;
        }
    }

    interface CI extends AI {
        default ReturnType foo(ArgumentType arg) {
            return null;
        }
    }

    interface DI extends BI, CI {
        ReturnType boo();

        default ReturnType foo(ArgumentType arg) {
            return BI.super.foo(arg);
        }
    }

    static class A implements AI {
        @Override
        public ReturnType foo() {
            return null;
        }

        private ReturnType privateFoo() {
            return null;
        }
    }

    static class B extends A implements DI {
        @Override
        public ReturnType boo() {
            return null;
        }

        @Override
        public ReturnType foo(ArgumentType arg) {
            return DI.super.foo(arg);
        }

        private ReturnType privateFoo() {
            return null;
        }
    }

    static class C extends B {
        @Override
        public ExtendedReturnType foo(ArgumentType arg) {
            return (ExtendedReturnType) super.foo(arg);
        }

        private ReturnType privateFoo() {
            return null;
        }
    }

    @SneakyThrows
    @Test
    void shouldReturnAllMethods() {
        //when & do
        var realMethods = ReflectionHelper.allMethods(C.class);

        //verify
        var sagaMethods = realMethods.map(MethodSagaMethodFactory::of).toList();
        assertThat(sagaMethods.subList(0, 3)).containsExactlyInAnyOrder(
            new SagaMethod(
                C.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            ),
            new SagaMethod(
                C.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ExtendedReturnType.class.getTypeName()
            ),
            new SagaMethod(
                C.class.getTypeName(),
                "privateFoo",
                List.of(),
                ReturnType.class.getTypeName()
            )
        );

        assertThat(sagaMethods.subList(3, 6)).containsExactlyInAnyOrder(
            new SagaMethod(
                B.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            ),
            new SagaMethod(
                B.class.getTypeName(),
                "privateFoo",
                List.of(),
                ReturnType.class.getTypeName()
            ),
            new SagaMethod(
                B.class.getTypeName(),
                "boo",
                List.of(),
                ReturnType.class.getTypeName()
            )
        );

        assertThat(sagaMethods.subList(6, 8)).containsExactlyInAnyOrder(
            new SagaMethod(
                DI.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            ),
            new SagaMethod(
                DI.class.getTypeName(),
                "boo",
                List.of(),
                ReturnType.class.getTypeName()
            )
        );

        assertThat(sagaMethods.subList(8, 10)).containsExactlyInAnyOrder(
            new SagaMethod(
                BI.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            ),
            new SagaMethod(
                CI.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            )
        );

        assertThat(sagaMethods.subList(10, 11)).containsExactlyInAnyOrder(
            new SagaMethod(
                AI.class.getTypeName(),
                "foo",
                List.of(),
                ReturnType.class.getTypeName()
            )
        );

        assertThat(sagaMethods.subList(11, 13)).containsExactlyInAnyOrder(
            new SagaMethod(
                A.class.getTypeName(),
                "foo",
                List.of(),
                ReturnType.class.getTypeName()
            ),
            new SagaMethod(
                A.class.getTypeName(),
                "privateFoo",
                List.of(),
                ReturnType.class.getTypeName()
            )
        );
    }

    @SneakyThrows
    @Test
    void shouldFindAllMethodsForLastOverride() {
        //when
        var lastOverrideMethod = C.class.getMethod("foo", ArgumentType.class);

        //do
        var allRealOverrideMethods = ReflectionHelper.findAllMethodsForLastOverride(lastOverrideMethod, C.class);

        //verify
        var sagaMethods = allRealOverrideMethods.stream().map(MethodSagaMethodFactory::of).toList();
        assertThat(sagaMethods.subList(0, 2)).containsExactlyInAnyOrder(
            new SagaMethod(
                C.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ExtendedReturnType.class.getTypeName()
            ),
            new SagaMethod(
                C.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            )
        );

        assertThat(sagaMethods.subList(2, 3)).containsExactlyInAnyOrder(
            new SagaMethod(
                B.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            )
        );

        assertThat(sagaMethods.subList(3, 4)).containsExactlyInAnyOrder(
            new SagaMethod(
                DI.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            )
        );

        assertThat(sagaMethods.subList(4, 6)).containsExactlyInAnyOrder(
            new SagaMethod(
                BI.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            ),
            new SagaMethod(
                CI.class.getTypeName(),
                "foo",
                List.of(ArgumentType.class.getTypeName()),
                ReturnType.class.getTypeName()
            )
        );
    }
}
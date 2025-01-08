package com.distributed_task_framework.saga.utils;

import com.distributed_task_framework.saga.functions.SagaBiConsumer;
import com.distributed_task_framework.saga.functions.SagaBiFunction;
import com.distributed_task_framework.saga.models.SagaMethod;
import lombok.SneakyThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class SerializableLambdaSagaMethodFactoryTest extends SagaMethodBase {

    @ParameterizedTest
    @MethodSource("consumerProvider")
    <T, U> void shouldParseConsumer(SagaBiConsumer<T, U> lambda, Method method, SagaMethod expected) {
        var serializedLambda = parse(lambda);
        var sagaMethod = SerializableLambdaSagaMethodFactory.of(serializedLambda);

        assertThat(sagaMethod).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("functionConsumer")
    <T, U, R> void shouldParseFunction(SagaBiFunction<T, U, R> lambda, Method method, SagaMethod expected) {
        var serializedLambda = parse(lambda);
        var sagaMethod = SerializableLambdaSagaMethodFactory.of(serializedLambda);

        assertThat(sagaMethod).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("functionSupplier")
    <T> void shouldParseSupplier(DebuggableSupplier<T> lambda, Method method, SagaMethod expected) {
        var serializedLambda = parse(lambda);
        var sagaMethod = SerializableLambdaSagaMethodFactory.of(serializedLambda);

        assertThat(sagaMethod).isEqualTo(expected);
    }

    @SneakyThrows
    private SerializedLambda parse(Serializable lambda) {
        Method m = lambda.getClass().getDeclaredMethod("writeReplace");
        m.setAccessible(true);
        Object replacement = m.invoke(lambda);
        if (!(replacement instanceof SerializedLambda)) {
            throw new RuntimeException();
        }
        return (SerializedLambda) replacement;
    }
}
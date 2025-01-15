package com.distributed_task_framework.saga.generator;

import com.distributed_task_framework.saga.functions.SagaBiConsumer;
import com.distributed_task_framework.saga.functions.SagaBiFunction;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.google.common.collect.Maps;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.util.Map;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TestSagaModelSpec<T> {
    T bean;
    @Nullable
    String name;
    @Nullable
    SagaSettings sagaSettings;
    @Nullable
    Boolean registerAllMethods;
    @Nullable
    SagaMethodSettings methodSettings;
    private final Map<SagaFunction<?, ?>, SagaMethodSettings> functionMethods;
    private final Map<SagaBiFunction<?, ?, ?>, SagaMethodSettings> biFunctionMethods;
    private final Map<SagaBiConsumer<?, ?>, SagaMethodSettings> sagaBiConsumerMethods;

    public static <T> TestSagaModelSpecBuilder<T> builder(T bean) {
        return new TestSagaModelSpecBuilder<>(bean);
    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    public static class TestSagaModelSpecBuilder<T> {
        private final T bean;
        @Nullable
        String name;
        @Nullable
        SagaSettings sagaSettings;
        @Nullable
        Boolean registerAllMethods;
        @Nullable
        SagaMethodSettings methodSettings;
        private final Map<SagaBiFunction<?, ?, ?>, SagaMethodSettings> biFunctionMethods = Maps.newHashMap();
        private final Map<SagaFunction<?, ?>, SagaMethodSettings> functionMethods = Maps.newHashMap();
        private final Map<SagaBiConsumer<?, ?>, SagaMethodSettings> sagaBiConsumerMethods = Maps.newHashMap();

        private TestSagaModelSpecBuilder(T bean) {
            this.bean = bean;
        }

        public TestSagaModelSpecBuilder<T> withName(String name) {
            this.name = name;
            return this;
        }

        public TestSagaModelSpecBuilder<T> withSagaSettings(SagaSettings sagaSettings) {
            this.sagaSettings = sagaSettings;
            return this;
        }

        public TestSagaModelSpecBuilder<T> withRegisterAllMethods(boolean registerAllMethods) {
            this.registerAllMethods = registerAllMethods;
            return this;
        }

        public <U, V> TestSagaModelSpecBuilder<T> withMethod(SagaFunction<U, V> method) {
            functionMethods.put(method, null);
            return this;
        }

        public <U, V> TestSagaModelSpecBuilder<T> withMethod(SagaFunction<U, V> method, SagaMethodSettings sagaMethodSettings) {
            functionMethods.put(method, sagaMethodSettings);
            return this;
        }

        public <U, V, R> TestSagaModelSpecBuilder<T> withMethod(SagaBiFunction<U, V, R> method) {
            biFunctionMethods.put(method, null);
            return this;
        }

        public <U, V, R> TestSagaModelSpecBuilder<T> withMethod(SagaBiFunction<U, V, R> method, SagaMethodSettings sagaMethodSettings) {
            biFunctionMethods.put(method, sagaMethodSettings);
            return this;
        }

        public <U, V> TestSagaModelSpecBuilder<T> withMethod(SagaBiConsumer<U, V> method) {
            sagaBiConsumerMethods.put(method, null);
            return this;
        }

        public <U, V> TestSagaModelSpecBuilder<T> withMethod(SagaBiConsumer<U, V> method, SagaMethodSettings sagaMethodSettings) {
            sagaBiConsumerMethods.put(method, sagaMethodSettings);
            return this;
        }

        public TestSagaModelSpecBuilder<T> withMethodSettings(SagaMethodSettings methodSettings) {
            this.methodSettings = methodSettings;
            return this;
        }

        public TestSagaModelSpec<T> build() {
            return new TestSagaModelSpec<>(
                bean,
                name,
                sagaSettings,
                registerAllMethods,
                methodSettings,
                functionMethods,
                biFunctionMethods,
                sagaBiConsumerMethods
            );
        }
    }
}

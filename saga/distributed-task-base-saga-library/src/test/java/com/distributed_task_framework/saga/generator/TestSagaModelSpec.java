package com.distributed_task_framework.saga.generator;

import com.distributed_task_framework.saga.functions.SagaBiConsumer;
import com.distributed_task_framework.saga.functions.SagaBiFunction;
import com.distributed_task_framework.saga.functions.SagaConsumer;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.utils.RunnableWithException;
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
    Boolean withoutSettings;
    boolean disableRegisterAllMethods;
    @Nullable
    SagaMethodSettings methodSettings;
    private final Map<SagaFunction<?, ?>, SagaMethodSettings> functionMethods;
    private final Map<SagaBiFunction<?, ?, ?>, SagaMethodSettings> biFunctionMethods;
    private final Map<SagaConsumer<?>, SagaMethodSettings> consumerMethods;
    private final Map<SagaBiConsumer<?, ?>, SagaMethodSettings> sagaBiConsumerMethods;
    private final Map<SagaFunction<?, ?>, BeforeTaskExecutionHandler> beforeTaskExecution;
    private final Map<SagaFunction<?, ?>, RunnableWithException> afterSagaMethodExecution;

    public static <T> TestSagaModelSpecBuilder<T> builder(T bean) {
        return new TestSagaModelSpecBuilder<>(bean);
    }

    public TestSagaModelSpecBuilder<T> toBuilder(T bean) {
        return new TestSagaModelSpecBuilder<>(
            bean,
            name,
            sagaSettings,
            withoutSettings,
            disableRegisterAllMethods,
            methodSettings,
            functionMethods,
            biFunctionMethods,
            consumerMethods,
            sagaBiConsumerMethods,
            beforeTaskExecution,
            afterSagaMethodExecution
        );
    }

    @AllArgsConstructor
    @FieldDefaults(level = AccessLevel.PRIVATE)
    public static class TestSagaModelSpecBuilder<T> {
        private final T bean;
        @Nullable
        String name;
        @Nullable
        SagaSettings sagaSettings;
        @Nullable
        Boolean withoutSettings;
        boolean disableRegisterAllMethods = false;
        @Nullable
        SagaMethodSettings methodSettings;
        private final Map<SagaFunction<?, ?>, SagaMethodSettings> functionMethods;
        private final Map<SagaBiFunction<?, ?, ?>, SagaMethodSettings> biFunctionMethods;
        private final Map<SagaConsumer<?>, SagaMethodSettings> consumerMethods;
        private final Map<SagaBiConsumer<?, ?>, SagaMethodSettings> sagaBiConsumerMethods;
        private final Map<SagaFunction<?, ?>, BeforeTaskExecutionHandler> beforeTaskExecution;
        private final Map<SagaFunction<?, ?>, RunnableWithException> afterSagaMethodExecution;

        private TestSagaModelSpecBuilder(T bean) {
            this.bean = bean;
            this.functionMethods = Maps.newHashMap();
            this.biFunctionMethods = Maps.newHashMap();
            this.consumerMethods = Maps.newHashMap();
            this.sagaBiConsumerMethods = Maps.newHashMap();
            this.beforeTaskExecution = Maps.newHashMap();
            this.afterSagaMethodExecution = Maps.newHashMap();
        }

        public TestSagaModelSpecBuilder<T> withName(String name) {
            this.name = name;
            return this;
        }

        public TestSagaModelSpecBuilder<T> withSagaSettings(SagaSettings sagaSettings) {
            this.sagaSettings = sagaSettings;
            return this;
        }

        public TestSagaModelSpecBuilder<T> withoutSettings() {
            this.withoutSettings = true;
            return this;
        }

        public TestSagaModelSpecBuilder<T> disableRegisterAllMethods() {
            this.disableRegisterAllMethods = true;
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

        public <U> TestSagaModelSpecBuilder<T> withMethod(SagaConsumer<U> method, SagaMethodSettings sagaMethodSettings) {
            consumerMethods.put(method, sagaMethodSettings);
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

        public <U, V> TestSagaModelSpecBuilder<T> doBeforeTaskExecution(SagaFunction<U, V> method,
                                                                        RunnableWithException runnable) {
            beforeTaskExecution.put(method, new BeforeTaskExecutionHandler(runnable, false));
            return this;
        }

        public <U, V> TestSagaModelSpecBuilder<T> doBeforeTaskExecutionOnSecondCall(SagaFunction<U, V> method,
                                                                                    RunnableWithException runnable) {
            beforeTaskExecution.put(method, new BeforeTaskExecutionHandler(runnable, true));
            return this;
        }

        public <U, V> TestSagaModelSpecBuilder<T> doAfterSagaMethodExecution(SagaFunction<U, V> method,
                                                                             RunnableWithException runnable) {
            afterSagaMethodExecution.put(method, runnable);
            return this;
        }

        public TestSagaModelSpec<T> build() {
            return new TestSagaModelSpec<>(
                bean,
                name,
                sagaSettings,
                withoutSettings,
                disableRegisterAllMethods,
                methodSettings,
                functionMethods,
                biFunctionMethods,
                consumerMethods,
                sagaBiConsumerMethods,
                beforeTaskExecution,
                afterSagaMethodExecution
            );
        }
    }

    public record BeforeTaskExecutionHandler(
        RunnableWithException runnable,
        boolean isLastCall
    ) {
    }
}

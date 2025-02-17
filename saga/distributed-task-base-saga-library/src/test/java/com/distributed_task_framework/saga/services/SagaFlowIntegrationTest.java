package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaCancellationException;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.Revert;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import com.distributed_task_framework.saga.settings.SagaSettings;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

//todo
public class SagaFlowIntegrationTest extends BaseSpringIntegrationTest {

    @Nested
    public class WaitCompletion {

        @SneakyThrows
        @Test
        public void shouldWaitCompletion() {
            //when
            var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaWithLocks());
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToConsume(testSagaModel.getBean()::method, 1)
                .start();

            //do
            testSagaModel.getBean().getCyclicBarrier().await();

            //verify
            assertThatThrownBy(() -> sagaFlow.waitCompletion(Duration.ofSeconds(3)))
                .isInstanceOf(TimeoutException.class);

            //do
            testSagaModel.getBean().getSagaMethodLock().countDown();

            //verify
            sagaFlow.waitCompletion(Duration.ofSeconds(3));
        }

        @SneakyThrows
        @Test
        public void shouldThrowSagaNotFoundExceptionWhenCompletedAndRemoved() {
            //when
            var sagaFlow = generateAndCreateSleepSaga(
                1,
                commonSettings -> commonSettings.toBuilder()
                    .availableAfterCompletionTimeout(Duration.ofMillis(1))
                    .build()
            );

            //do
            waitFor(() -> !sagaRepository.existsById(sagaFlow.trackId()));

            //verify
            assertThatThrownBy(sagaFlow::waitCompletion).isInstanceOf(SagaNotFoundException.class);
        }

        @SneakyThrows
        @Test
        public void shouldThrowSagaNotFoundExceptionWhenExpired() {
            //when
            var sagaFlow = generateAndCreateSleepSaga(
                100_000,
                commonSettings -> commonSettings.toBuilder()
                    .expirationTimeout(Duration.ofSeconds(1))
                    .build()
            );

            //do
            waitFor(() -> !sagaRepository.existsById(sagaFlow.trackId()));

            //verify
            assertThatThrownBy(sagaFlow::waitCompletion).isInstanceOf(SagaNotFoundException.class);
        }

        @SneakyThrows
        @Test
        public void shouldThrowTimeoutException() {
            //when
            var sagaFlow = generateAndCreateSleepSaga(100_000, Function.identity());

            //verify
            assertThatThrownBy(() -> sagaFlow.waitCompletion(Duration.ofSeconds(1)))
                .isInstanceOf(TimeoutException.class);
        }

        @SneakyThrows
        @Test
        public void shouldThrowSagaExecutionOnlyAfterRevert() {
            //when
            @Getter
            @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
            class TestSaga {
                CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
                CountDownLatch sagaRevertLock = new CountDownLatch(1);

                public void method(int input) {
                    throw new TestUserUncheckedException();
                }

                @Revert
                public void revertMethod(int input, @Nullable SagaExecutionException throwable) throws Exception {
                    cyclicBarrier.await();
                    sagaRevertLock.await();
                }
            }
            var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSaga());
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToConsume(
                    testSagaModel.getBean()::method,
                    testSagaModel.getBean()::revertMethod,
                    1
                )
                .start();

            //do
            testSagaModel.getBean().getCyclicBarrier().await();

            //verify
            assertThatThrownBy(() -> sagaFlow.waitCompletion(Duration.ofSeconds(1)))
                .isInstanceOf(TimeoutException.class);

            //do
            testSagaModel.getBean().getSagaRevertLock().countDown();

            //verify
            assertThatThrownBy(sagaFlow::waitCompletion)
                .isInstanceOf(SagaExecutionException.class)
                .hasCauseInstanceOf(TestUserUncheckedException.class);
        }

        @SneakyThrows
        @Test
        public void shouldThrowSagaCancellationExceptionOnlyAfterRevert() {
            //when
            var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaWithLocks());
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToConsume(
                    testSagaModel.getBean()::method,
                    testSagaModel.getBean()::revertMethod,
                    1
                )
                .start();

            //do
            testSagaModel.getBean().getCyclicBarrier().await();
            sagaManager.cancel(sagaFlow.trackId());
            testSagaModel.getBean().getSagaMethodLock().countDown();

            //verify
            assertThatThrownBy(() -> sagaFlow.waitCompletion(Duration.ofSeconds(1)))
                .isInstanceOf(TimeoutException.class);

            //do
            testSagaModel.getBean().getSagaRevertLock().countDown();

            //verify
            assertThatThrownBy(sagaFlow::waitCompletion).isInstanceOf(SagaCancellationException.class);
        }

        private SagaFlow<Integer> generateAndCreateSleepSaga(int delay,
                                                             Function<SagaSettings, SagaSettings> settingCustomizer) {
            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(new TestSagaBase(10))
                .withRegisterAllMethods(true)
                .withSagaSettings(settingCustomizer.apply(SagaSettings.DEFAULT))
                .build()
            );
            return distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sleep, delay)
                .start();
        }

        @Getter
        @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
        static class TestSagaWithLocks {
            CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
            CountDownLatch sagaMethodLock = new CountDownLatch(1);
            CountDownLatch sagaRevertLock = new CountDownLatch(1);

            public void method(int input) throws Exception {
                cyclicBarrier.await();
                sagaMethodLock.await();
            }

            @Revert
            public void revertMethod(int input, @Nullable SagaExecutionException throwable) throws Exception {
                sagaRevertLock.await();
            }
        }
    }

    @Nested
    public class GetResult {

        @SneakyThrows
        @Test
        public void shouldReturnResult() {
            //when
            var sagaFlow = generateAndCreateSagaWithResult(Function.identity());

            //do
            var optionalResult = sagaFlow.get();

            //verify
            assertThat(optionalResult)
                .isPresent()
                .get()
                .isEqualTo(800);
        }

        @SneakyThrows
        @Test
        public void shouldThrowSagaNotFoundExceptionWhenCompletedAndRemoved() {
            //when
            var sagaFlow = generateAndCreateSagaWithResult(sagaSettings -> sagaSettings.toBuilder()
                .availableAfterCompletionTimeout(Duration.ofMillis(1))
                .build()
            );

            //do
            waitFor(() -> !sagaRepository.existsById(sagaFlow.trackId()));

            //verify
            assertThatThrownBy(sagaFlow::get).isInstanceOf(SagaNotFoundException.class);
        }

        @SneakyThrows
        @Test
        public void shouldThrowSagaNotFoundExceptionWhenExpired() {
            //when
            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(new TestSagaBase(10))
                .withRegisterAllMethods(true)
                .withSagaSettings(SagaSettings.DEFAULT.toBuilder()
                    .expirationTimeout(Duration.ofSeconds(1))
                    .build()
                )
                .build()
            );
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sleep, 100_000)
                .start();

            //do
            waitFor(() -> !sagaRepository.existsById(sagaFlow.trackId()));

            //verify
            assertThatThrownBy(sagaFlow::get).isInstanceOf(SagaNotFoundException.class);
        }

        @SneakyThrows
        @Test
        public void shouldThrowTimeoutException() {
            //when
            var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaBase(10));
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sleep, 100_000)
                .start();

            //verify
            assertThatThrownBy(() -> sagaFlow.get(Duration.ofSeconds(1)))
                .isInstanceOf(TimeoutException.class);
        }

        @Test
        @SneakyThrows
        public void shouldThrowSagaExecution() {
            //when
            var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaBase(10));

            //do
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::justThrowExceptionAsFunction, 10)
                .start();

            //verify
            assertThatThrownBy(sagaFlow::get)
                .isInstanceOf(SagaExecutionException.class)
                .hasCauseInstanceOf(TestUserUncheckedException.class);
        }

        @Test
        @SneakyThrows
        public void shouldThrowSagaCancellationException() {
            //when
            var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaBase(10));
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sleep, 100_000)
                .start();

            //do
            sagaManager.cancel(sagaFlow.trackId());

            //verify
            assertThatThrownBy(sagaFlow::get).isInstanceOf(SagaCancellationException.class);
        }

        private SagaFlow<Integer> generateAndCreateSagaWithResult(Function<SagaSettings, SagaSettings> settingCustomizer) {
            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(new TestSagaBase(10))
                .withRegisterAllMethods(true)
                .withSagaSettings(settingCustomizer.apply(SagaSettings.DEFAULT))
                .build()
            );
            return distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
                .thenRun(testSagaModel.getBean()::multiplyAsFunction)
                .thenRun(testSagaModel.getBean()::sumAsFunction)
                .start();
        }
    }

    @Nested
    public class IsCompleted {
        //todo
    }

    @Nested
    public class IsCanceled {
        //todo
    }

    @Nested
    public class Cancel {
        //todo

    }
}

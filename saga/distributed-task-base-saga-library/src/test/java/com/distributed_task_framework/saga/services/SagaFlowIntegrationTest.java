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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

//todo
public class SagaFlowIntegrationTest extends BaseSpringIntegrationTest {

    @Nested
    public class WaitCompletion {

        //todo:
        public void shouldWaitCompletion() {

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
        public void shouldThrowSagaExecutionExceptionOnlyAfterRevert() {
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
            @Getter
            @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
            class TestSaga {
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
    }

    @Nested
    public class GetCompletion {
        //todo
    }

    @Nested
    public class IsCompleted {
        //todo
    }

    @Nested
    public class Cancel {
        //todo

        //todo: SagaCancellationException in get() method
    }


}

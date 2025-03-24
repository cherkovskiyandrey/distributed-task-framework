package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaCancellationException;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.Revert;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import com.distributed_task_framework.utils.ConsumerWithException;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils.withAvailableAfterCompletionTimeout;
import static com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils.withExpirationTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

//todo
public class SagaFlowIntegrationTest extends BaseSpringIntegrationTest {

    @Nested
    public class WaitCompletion {

        @SneakyThrows
        @Test
        public void shouldWaitCompletion() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaWithLocks());
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
        public void shouldThrowTimeoutException() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaBase(10));
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sleep, 100_000)
                .start();

            //verify
            assertThatThrownBy(() -> sagaFlow.waitCompletion(Duration.ofSeconds(1)))
                .isInstanceOf(TimeoutException.class);
        }

        @SneakyThrows
        @Test
        public void shouldThrowSagaExecutionOnlyAfterRevert() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaWithLockInRevert());
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToConsume(
                    testSagaModel.getBean()::methodAsConsumer,
                    testSagaModel.getBean()::revertMethodAsConsumer,
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
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaWithLocks());
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
    }

    @Nested
    public class GetResult {

        @SneakyThrows
        @Test
        public void shouldReturnResult() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaBase(10));
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
                .thenRun(testSagaModel.getBean()::multiplyAsFunction)
                .thenRun(testSagaModel.getBean()::sumAsFunction)
                .start();

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
        public void shouldThrowTimeoutException() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaBase(10));
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sleep, 100_000)
                .start();

            //verify
            assertThatThrownBy(() -> sagaFlow.get(Duration.ofSeconds(1)))
                .isInstanceOf(TimeoutException.class);
        }

        @Test
        @SneakyThrows
        public void shouldThrowSagaExecutionOnlyAfterRevert() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaWithLockInRevert());
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(
                    testSagaModel.getBean()::methodAsFunction,
                    testSagaModel.getBean()::revertMethodAsFunction,
                    1
                )
                .start();

            //do
            testSagaModel.getBean().getCyclicBarrier().await();

            //verify
            assertThatThrownBy(() -> sagaFlow.get(Duration.ofSeconds(3)))
                .isInstanceOf(TimeoutException.class);

            //do
            testSagaModel.getBean().getSagaRevertLock().countDown();

            //verify
            await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                    assertThatThrownBy(sagaFlow::get)
                        .isInstanceOf(SagaExecutionException.class)
                        .hasCauseInstanceOf(TestUserUncheckedException.class)
                );
        }

        @Test
        @SneakyThrows
        public void shouldThrowSagaCancellationException() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaBase(10));
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sleep, 100_000)
                .start();

            //do
            sagaManager.cancel(sagaFlow.trackId());

            //verify
            assertThatThrownBy(sagaFlow::get).isInstanceOf(SagaCancellationException.class);
        }
    }

    @Nested
    public class IsCompleted {

        @SneakyThrows
        @Test
        public void shouldReturnRelevantResult() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaWithLocks());
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToConsume(testSagaModel.getBean()::method, 1)
                .start();

            //do
            testSagaModel.getBean().getCyclicBarrier().await();

            //verify
            assertThat(sagaFlow.isCompleted()).isFalse();

            //do
            testSagaModel.getBean().getSagaMethodLock().countDown();

            //verify
            waitFor(sagaFlow::isCompleted);
        }

        @SneakyThrows
        @Test
        public void shouldReturnTrueOnlyAfterRevert() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaWithLockInRevert());
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToConsume(
                    testSagaModel.getBean()::methodAsConsumer,
                    testSagaModel.getBean()::revertMethodAsConsumer,
                    1
                )
                .start();

            //do
            testSagaModel.getBean().getCyclicBarrier().await();

            //verify
            assertThat(sagaFlow.isCompleted()).isFalse();

            //do
            testSagaModel.getBean().getSagaRevertLock().countDown();

            //verify
            waitFor(sagaFlow::isCompleted);
        }
    }

    @Nested
    public class IsCanceled {

        @SneakyThrows
        @Test
        void shouldReturnTrueBeforeRevertIsInvoking() {
            //when
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaWithLocks());
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToConsume(testSagaModel.getBean()::method, 1)
                .start();

            //do
            testSagaModel.getBean().getCyclicBarrier().await();

            //verify
            assertThat(sagaFlow.isCanceled()).isFalse();

            //do
            sagaManager.cancel(sagaFlow.trackId());
            testSagaModel.getBean().getSagaRevertLock().countDown();

            //verify
            assertThat(sagaFlow.isCanceled()).isTrue();

            //do
            testSagaModel.getBean().getSagaRevertLock().countDown();
        }
    }

    //todo
    @Nested
    public class Cancel {

        @SneakyThrows
        @Test
        public void shouldCompleteWhenGracefullyCanceledBeforeExecutionAndOneStep() {
            //when
            var testSaga = Mockito.spy(new TestSagaBase(10));
            var barrier = new CyclicBarrier(2);
            var locker = new CountDownLatch(1);
            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSaga)
                .doBeforeTaskExecution(testSaga::sumAsFunction, () -> {
                        barrier.await();
                        locker.await();
                    }
                )
                .build()
            );
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(
                    testSagaModel.getBean()::sumAsFunction,
                    testSagaModel.getBean()::diffForFunction,
                    10
                )
                .start();

            //do
            barrier.await();
            sagaFlow.cancel(true);
            locker.countDown();
            waitFor(() -> sagaRepository.isCompleted(sagaFlow.trackId()).orElse(true));

            //verify
            verify(testSaga, never()).diffForFunction(anyInt(), any(), any());
            assertThatThrownBy(sagaFlow::get).isInstanceOf(SagaCancellationException.class);
            assertThat(testSaga.getValue()).isEqualTo(10);
        }

        @SneakyThrows
        @Test
        public void shouldRunRevertFlowFromPrevPositionWhenGracefullyCanceledBeforeExecution() {
            //when
            var testSaga = Mockito.spy(new TestSagaBase(10));
            var barrier = new CyclicBarrier(2);
            var locker = new CountDownLatch(1);
            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSaga)
                .doBeforeTaskExecution(testSaga::multiplyAsFunction, () -> {
                        barrier.await();
                        locker.await();
                    }
                )
                .build()
            );
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(
                    testSagaModel.getBean()::sumAsFunction,
                    testSagaModel.getBean()::diffForFunction,
                    10
                )
                .thenRun(
                    testSagaModel.getBean()::multiplyAsFunction,
                    testSagaModel.getBean()::divideForFunction
                )
                .start();

            //do
            barrier.await();
            sagaFlow.cancel(true);
            locker.countDown();
            waitFor(() -> sagaRepository.isCompleted(sagaFlow.trackId()).orElse(true));

            //verify
            verify(testSaga, never()).divideForFunction(anyInt(), any(), any());
            assertThatThrownBy(sagaFlow::get).isInstanceOf(SagaCancellationException.class);
            assertThat(testSaga.getValue()).isEqualTo(10);
        }

        @Test
        @SneakyThrows
        public void shouldRunRevertFlowFromCurPositionWhenGracefullyCanceledBeforeExecutionOnSecondAttempt() {
            //when
            var testSaga = Mockito.spy(new TestSagaBase(10));
            var barrier = new CyclicBarrier(2);
            var locker = new CountDownLatch(1);
            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSaga)
                .withMethodSettings(TestSagaGeneratorUtils.withRetry(2))
                .doBeforeTaskExecutionOnSecondCall(
                    testSaga::multiplyAsFunctionWithException,
                    () -> {
                        barrier.await();
                        locker.await();
                    }
                )
                .build()
            );
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(
                    testSagaModel.getBean()::sumAsFunction,
                    testSagaModel.getBean()::diffForFunction,
                    10
                )
                .thenRun(
                    testSagaModel.getBean()::multiplyAsFunctionWithException,
                    testSagaModel.getBean()::divideForFunctionWithExceptionHandling
                )
                .start();

            //do
            barrier.await();
            sagaFlow.cancel(true);
            locker.countDown();
            waitFor(() -> sagaRepository.isCompleted(sagaFlow.trackId()).orElse(true));

            //verify
            verify(testSaga).divideForFunctionWithExceptionHandling(anyInt(), any(), any());
            assertThatThrownBy(sagaFlow::get).isInstanceOf(SagaCancellationException.class);
            assertThat(testSaga.getValue()).isEqualTo(10);
        }

        //todo: fix!!!!
        //todo: + spy SagaTaskFactory has to be reinit after each call!!
        @Test
        @SneakyThrows
        public void shouldRunRevertFlowFromCurPositionWhenGracefullyCanceledDuringExecutionFirstMethod() {
            //when
            var testSaga = Mockito.spy(new TestSagaBase(10));
            var barrier = new CyclicBarrier(2);
            var locker = new CountDownLatch(1);
            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSaga)
                .doAfterSagaMethodExecution(
                    testSaga::sumAsFunction,
                    () -> {
                        barrier.await();
                        locker.await();
                    }
                )
                .build()
            );
            var sagaFlow = distributionSagaService.create(testSagaModel.getName())
                .registerToRun(
                    testSagaModel.getBean()::sumAsFunction,
                    testSagaModel.getBean()::diffForFunction,
                    10
                )
                .start();

            //do
            barrier.await();
            sagaFlow.cancel(true);
            locker.countDown();
            waitFor(() -> sagaRepository.isCompleted(sagaFlow.trackId()).orElse(true));

            //verify
            verify(testSaga).diffForFunction(anyInt(), any(), any());
            assertThatThrownBy(sagaFlow::get).isInstanceOf(SagaCancellationException.class);
            assertThat(testSaga.getValue()).isEqualTo(10);
        }

        //todo: cancel not gracefully
    }

    private static Stream<Arguments> sagaNotFoundExceptionSourceProvider() {
        return Stream.of(
            Arguments.of((ConsumerWithException<SagaFlow<Integer>, Exception>) SagaFlow::waitCompletion),
            Arguments.of((ConsumerWithException<SagaFlow<Integer>, Exception>) SagaFlow::get),
            Arguments.of((ConsumerWithException<SagaFlow<Integer>, Exception>) SagaFlow::isCompleted),
            Arguments.of((ConsumerWithException<SagaFlow<Integer>, Exception>) SagaFlow::isCanceled)
        );
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("sagaNotFoundExceptionSourceProvider")
    public void shouldThrowSagaNotFoundExceptionWhenCompletedAndRemoved(ConsumerWithException<SagaFlow<Integer>, Exception> consumer) {
        //when
        var testSagaModel = testSagaGenerator.generate(
            TestSagaModelSpec.builder(new TestSagaBase(10))
                .withSagaSettings(withAvailableAfterCompletionTimeout(Duration.ofMillis(1)))
                .build()

        );
        var sagaFlow = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
            .start();


        //do
        waitFor(() -> !sagaRepository.existsById(sagaFlow.trackId()));

        //verify
        assertThatThrownBy(() -> consumer.accept(sagaFlow)).isInstanceOf(SagaNotFoundException.class);
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("sagaNotFoundExceptionSourceProvider")
    public void shouldThrowSagaNotFoundExceptionWhenExpired(ConsumerWithException<SagaFlow<Integer>, Exception> consumer) {
        //when
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(new TestSagaBase(10))
            .withSagaSettings(withExpirationTimeout(Duration.ofSeconds(1)))
            .build()
        );
        var sagaFlow = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sleep, 100_000)
            .start();

        //do
        waitFor(() -> !sagaRepository.existsById(sagaFlow.trackId()));

        //verify
        assertThatThrownBy(() -> consumer.accept(sagaFlow)).isInstanceOf(SagaNotFoundException.class);
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

    @Getter
    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    static class TestSagaWithLockInRevert {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        CountDownLatch sagaRevertLock = new CountDownLatch(1);

        public void methodAsConsumer(int input) {
            throw new TestUserUncheckedException();
        }

        @Revert
        public void revertMethodAsConsumer(int input, @Nullable SagaExecutionException throwable) throws Exception {
            cyclicBarrier.await();
            sagaRevertLock.await();
        }

        public int methodAsFunction(int input) {
            throw new TestUserUncheckedException();
        }

        @Revert
        public void revertMethodAsFunction(int input,
                                           @Nullable Integer parentOutput,
                                           @Nullable SagaExecutionException throwable) throws Exception {
            cyclicBarrier.await();
            sagaRevertLock.await();
        }
    }
}

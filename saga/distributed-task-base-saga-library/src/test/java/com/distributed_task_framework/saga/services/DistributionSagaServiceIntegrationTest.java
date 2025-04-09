package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import com.distributed_task_framework.saga.settings.SagaSettings;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class DistributionSagaServiceIntegrationTest extends BaseSpringIntegrationTest {

    @Nested
    public class CreateTest {

        @Test
        public void shouldCreateWhenWithDefaultSettings() {
            //when
            setFixedTime();
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaBase(100));

            //do
            distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
                .start();

            //verify
            assertThat(sagaRepository.findAll().iterator().next())
                .matches(sagaEntity -> Objects.equals(
                        sagaEntity.getExpirationDateUtc(),
                        LocalDateTime.now(clock).plus(SagaSettings.DEFAULT.getExpirationTimeout())
                    ),
                    "expirationDate"
                )
                .matches(sagaEntity -> Objects.equals(
                        sagaEntity.getAvailableAfterCompletionTimeoutSec(),
                        SagaSettings.DEFAULT.getAvailableAfterCompletionTimeout().toSeconds()
                    ),
                    "availableAfterCompletionTimeoutSec"
                );
        }

        @Test
        public void shouldCreateWhenWithOverrideDefaultSettings() {
            //when
            setFixedTime();
            var expirationTimeout = Duration.ofMinutes(20);
            var availableAfterCompletionTimeout = Duration.ofMinutes(35);
            distributionSagaService.registerDefaultSagaSettings(SagaSettings.DEFAULT.toBuilder()
                .expirationTimeout(expirationTimeout)
                .availableAfterCompletionTimeout(availableAfterCompletionTimeout)
                .build()
            );
            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(new TestSagaBase(100))
                .withoutSettings()
                .build()
            );

            //do
            distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
                .start();

            //verify
            assertThat(sagaRepository.findAll().iterator().next())
                .matches(sagaEntity -> Objects.equals(
                        sagaEntity.getExpirationDateUtc(),
                        LocalDateTime.now(clock).plus(expirationTimeout)
                    ),
                    "expirationDate"
                )
                .matches(sagaEntity -> Objects.equals(
                        sagaEntity.getAvailableAfterCompletionTimeoutSec(),
                        availableAfterCompletionTimeout.toSeconds()
                    ),
                    "availableAfterCompletionTimeoutSec"
                );
        }

        @Test
        public void shouldCreateWhenWithPredefinedSettings() {
            //when
            setFixedTime();
            var expirationTimeout = Duration.ofMinutes(20);
            var availableAfterCompletionTimeout = Duration.ofMinutes(35);
            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(new TestSagaBase(100))
                .withoutSettings()
                .build()
            );
            distributionSagaService.registerSagaSettings(
                testSagaModel.getName(),
                SagaSettings.DEFAULT.toBuilder()
                    .expirationTimeout(expirationTimeout)
                    .availableAfterCompletionTimeout(availableAfterCompletionTimeout)
                    .build()
            );

            //do
            distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
                .start();

            //verify
            assertThat(sagaRepository.findAll().iterator().next())
                .matches(sagaEntity -> Objects.equals(
                        sagaEntity.getExpirationDateUtc(),
                        LocalDateTime.now(clock).plus(expirationTimeout)
                    ),
                    "expirationDate"
                )
                .matches(sagaEntity -> Objects.equals(
                        sagaEntity.getAvailableAfterCompletionTimeoutSec(),
                        availableAfterCompletionTimeout.toSeconds()
                    ),
                    "availableAfterCompletionTimeoutSec"
                );
        }

        @Test
        public void shouldCreateWhenWithCustomSettings() {
            //when
            setFixedTime();
            var testSagaModel = testSagaGenerator.generateFor(new TestSagaBase(100));

            //do
            distributionSagaService.create(
                    testSagaModel.getName(),
                    SagaSettings.DEFAULT.toBuilder()
                        .expirationTimeout(Duration.ofSeconds(1000))
                        .availableAfterCompletionTimeout(Duration.ofSeconds(100))
                        .build()
                )
                .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
                .start();

            //verify
            assertThat(sagaRepository.findAll().iterator().next())
                .matches(sagaEntity -> Objects.equals(
                        sagaEntity.getExpirationDateUtc(),
                        LocalDateTime.now(clock).plusSeconds(1000)
                    ),
                    "expirationDate"
                )
                .matches(sagaEntity -> Objects.equals(
                        sagaEntity.getAvailableAfterCompletionTimeoutSec(),
                        100L
                    ),
                    "availableAfterCompletionTimeoutSec"
                );
        }
    }

    @Nested
    public class CreateWithAffinityTest {

        @SneakyThrows
        @Test
        public void shouldDoSequentiallyWhenCreateWithAffinity() {
            //when
            @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
            class TestSaga {
                CyclicBarrier firstSagaRun = new CyclicBarrier(2);
                CountDownLatch firstSagaLock = new CountDownLatch(1);
                CountDownLatch secondSagaCompletionSignal = new CountDownLatch(1);

                public void firstSagaFirstMethod(int dummy) throws BrokenBarrierException, InterruptedException {
                    firstSagaRun.await();
                    firstSagaLock.await();
                }

                public void firstSagaSecondMethod(int dummy) {
                }

                public void secondSagaFirstMethod(int dummy) {
                }

                public void secondSagaSecondMethod(int dummy) {
                    secondSagaCompletionSignal.countDown();
                }

                public void waitForFirstSagaStart() throws BrokenBarrierException, InterruptedException {
                    firstSagaRun.await();
                }

                public boolean waitForSecondSagaCompletion(int timeout) throws InterruptedException {
                    return secondSagaCompletionSignal.await(timeout, TimeUnit.SECONDS);
                }

                public void unlockFirstSaga() {
                    firstSagaLock.countDown();
                }
            }
            var testSaga = new TestSaga();
            var testSagaModel = testSagaGenerator.generateFor(testSaga);

            //do
            distributionSagaService.createWithAffinity(testSagaModel.getName(), "afg", "aff")
                .registerToConsume(testSagaModel.getBean()::firstSagaFirstMethod, 1)
                .thenConsume(testSagaModel.getBean()::firstSagaSecondMethod)
                .start();
            TimeUnit.MILLISECONDS.sleep(100);
            distributionSagaService.createWithAffinity(testSagaModel.getName(), "afg", "aff")
                .registerToConsume(testSagaModel.getBean()::secondSagaFirstMethod, 1)
                .thenConsume(testSagaModel.getBean()::secondSagaSecondMethod)
                .start();

            //verify
            testSaga.waitForFirstSagaStart();
            assertThat(testSaga.waitForSecondSagaCompletion(5)).isFalse();
            testSaga.unlockFirstSaga();
            testSaga.waitForSecondSagaCompletion(1);
        }
    }

    @Nested
    public class GetFlowTest {

        @Test
        public void shouldGetFlow() {
            //when
            var trackId = generateAndRegisterSaga(1, Function.identity());

            //do
            var result = distributionSagaService.getFlow(trackId);

            //verify
            assertThat(result)
                .isNotNull()
                .matches(r -> Objects.equals(r.trackId(), trackId));
        }

        @SuppressWarnings("AssertBetweenInconvertibleTypes")
        @Test
        public void shouldGetFlowWithResult() {
            //when
            var trackId = generateAndRegisterSaga(10, Function.identity());

            //do
            var result = distributionSagaService.getFlow(trackId, Integer.class);

            //verify
            assertThat(result)
                .isNotNull()
                .satisfies(r -> assertThat(r.get())
                    .isPresent()
                    .get()
                    .isEqualTo(10)
                );
        }

        @Test
        public void shouldThrowSagaNotFoundExceptionWhenNotExists() {
            //do & verify
            assertThatThrownBy(() -> distributionSagaService.getFlow(UUID.randomUUID()))
                .isInstanceOf(SagaNotFoundException.class);
        }

        @SneakyThrows
        @Test
        public void shouldThrowSagaNotFoundExceptionWhenCompletedAndRemoved() {
            //when
            var trackId = generateAndRegisterSaga(
                1,
                commonSettings -> commonSettings.toBuilder()
                    .availableAfterCompletionTimeout(Duration.ofMillis(1))
                    .build()
            );

            //do
            waitFor(() -> !sagaRepository.existsById(trackId));

            //verify
            assertThatThrownBy(() -> distributionSagaService.getFlow(trackId))
                .isInstanceOf(SagaNotFoundException.class);
        }

        @SneakyThrows
        @Test
        public void shouldThrowSagaNotFoundExceptionWhenExpired() {
            //when
            var trackId = generateAndRegisterSaga(
                100_000,
                commonSettings -> commonSettings.toBuilder()
                    .expirationTimeout(Duration.ofSeconds(1))
                    .build()
            );

            //do
            waitFor(() -> !sagaRepository.existsById(trackId));

            //verify
            assertThatThrownBy(() -> distributionSagaService.getFlow(trackId))
                .isInstanceOf(SagaNotFoundException.class);
        }

        private UUID generateAndRegisterSaga(int delay, Function<SagaSettings, SagaSettings> settingCustomizer) {
            record TestSaga() {
                @SneakyThrows
                public int sleep(int i) {
                    TimeUnit.MILLISECONDS.sleep(i);
                    return i;
                }
            }

            var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(new TestSaga())
                .withSagaSettings(settingCustomizer.apply(SagaSettings.DEFAULT))
                .build()
            );
            distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sleep, delay)
                .start();

            return sagaRepository.findAll().iterator().next().getSagaId();
        }
    }
}
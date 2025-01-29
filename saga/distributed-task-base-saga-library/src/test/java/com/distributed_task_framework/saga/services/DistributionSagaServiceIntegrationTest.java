package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import com.distributed_task_framework.saga.settings.SagaSettings;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

//todo: create
//todo: createWithAffinity
class DistributionSagaServiceIntegrationTest extends BaseSpringIntegrationTest {

    @Nested
    public class CreateTest {

        @Test
        public void shouldCreateWhenWithCustomSettings() {
            //when
            setFixedTime();
            var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSaga(100));

            //do
            distributionSagaService.create(
                    testSagaModel.getName(),
                    SagaSettings.DEFAULT.toBuilder()
                        .expirationTimeout(Duration.ofSeconds(1000))
                        .availableAfterCompletionTimeout(Duration.ofSeconds(100))
                        .build()
                )
                .registerToRun(testSagaModel.getBean()::sum, 10)
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
                    "AvailableAfterCompletionTimeoutSec"
                );
        }

        //todo
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
                .matches(r -> Objects.equals(r.trackId(), trackId), "trackId")
                .satisfies(r -> assertThat(r.get())
                    .isPresent()
                    .get()
                    .isEqualTo(10)
                );
        }

        @Test
        public void shouldNotGetFlowWhenNotExists() {
            //do & verify
            assertThatThrownBy(() -> distributionSagaService.getFlow(UUID.randomUUID()))
                .isInstanceOf(SagaNotFoundException.class);
        }

        @SneakyThrows
        @Test
        public void shouldNotGetFlowWhenCompletedAndRemoved() {
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
        public void shouldNotGetFlowWhenExpired() {
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
                .withRegisterAllMethods(true)
                .withSagaSettings(settingCustomizer.apply(SagaSettings.DEFAULT))
                .build()
            );
            distributionSagaService.create(testSagaModel.getName())
                .registerToRun(testSagaModel.getBean()::sleep, delay)
                .start();

            return sagaRepository.findAll().iterator().next().getSagaId();
        }
    }


    //todo: move to groups


    @SneakyThrows
    @Test
    void shouldExecuteWhenHiddenMethod() {
        //when
        record TestSaga(int delta) {
            private int sum(int i) {
                return i + delta;
            }
        }
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSaga(10));

        //do
        var resultOpt = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sum, 10)
            .start()
            .get();

        //verify
        assertThat(resultOpt)
            .isPresent()
            .get()
            .isEqualTo(20);
    }

    @SneakyThrows
    @Test
    void shouldExecuteWhenSequenceOfMethod() {
        //when
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSaga(0));

        //do
        var resultOpt = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sum, 10)
            .thenRun(testSagaModel.getBean()::multiply)
            .thenRun(testSagaModel.getBean()::divide)
            .start()
            .get();

        //verify
        assertThat(resultOpt)
            .isPresent()
            .get()
            .isEqualTo(40);
    }

    //todo: rewrite
    @SneakyThrows
    @Test
    void shouldNotRetryWhenNoRetryFor() {
        //when
        class TestSagaNotRetry extends TestSaga {
            public TestSagaNotRetry(int value) {
                super(value);
            }

            @Override
            public int sum(int i) {
                super.sum(i);
                throw new TestUserUncheckedException();
            }
        }

        var testSagaNoRetryFor = new TestSagaNotRetry(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaNoRetryFor)
            .withRegisterAllMethods(true)
            .withMethod(
                testSagaNoRetryFor::sum,
                TestSagaGeneratorUtils.withNoRetryFor(TestUserUncheckedException.class)
            )
            .build()
        );

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sum, 5)
            .start()
            .waitCompletion();

        //verify
        assertThat(testSagaNoRetryFor.getValue()).isEqualTo(105);
    }


    @Getter
    public static class TestSaga {
        private int value;

        public TestSaga(int value) {
            this.value = value;
        }

        public int sum(int i) {
            value += i;
            return i;
        }

        public int diff(int i) {
            return value - i;
        }

        public int multiply(int i) {
            return i * 10;
        }

        public int divide(int i) {
            return i / 5;
        }
    }


    //todo: serialisation of complex type

}
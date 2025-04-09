package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SagaIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldExecuteWhenHiddenMethod() {
        //when
        @RequiredArgsConstructor
        class TestSaga {
            private final int delta;

            private int sum(int i) {
                return i + delta;
            }
        }
        var testSagaModel = testSagaGenerator.generateFor(new TestSaga(10));

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
    void shouldNotRetryWhenNoRetryFor() {
        //when
        var testSagaNoRetryFor = new TestSagaBase(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaNoRetryFor)
            .withMethod(
                testSagaNoRetryFor::sumAsConsumerWithException,
                TestSagaGeneratorUtils.withNoRetryFor(TestUserUncheckedException.class)
            )
            .build()
        );

        //do
        assertThatThrownBy(() -> {
            distributionSagaService.create(testSagaModel.getName())
                .registerToConsume(testSagaModel.getBean()::sumAsConsumerWithException, 5)
                .start()
                .waitCompletion();
        })
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaNoRetryFor.getValue()).isEqualTo(105);
    }

    //todo: serialisation of complex type
}

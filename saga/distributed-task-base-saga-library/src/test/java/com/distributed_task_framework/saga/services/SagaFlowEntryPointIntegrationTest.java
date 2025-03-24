package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaFlowEntryPointIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldRegisterToRun() {
        //when
        var testSagaModel = testSagaGenerator.generateFor(new TestSagaBase(10));

        //do
        var resultOpt = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
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
    void shouldRegisterToRunWhenWithRevert() {
        //when
        var testSagaException = new TestSagaBase(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withMethod(testSagaException::sumAsFunctionWithException, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sumAsFunctionWithException,
                testSagaModel.getBean()::diffForFunctionWithExceptionHandling,
                5
            )
            .start()
            .get()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(100);
    }

    @SneakyThrows
    @Test
    void shouldRegisterToConsume() {
        //when
        var testSagaModel = testSagaGenerator.generateFor(new TestSagaBase(0));

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(testSagaModel.getBean()::sumAsConsumer, 100)
            .start()
            .waitCompletion();

        //verify
        assertThat(testSagaModel.getBean().getValue()).isEqualTo(100);
    }

    @SneakyThrows
    @Test
    void shouldRegisterToConsumeWhenWithRevert() {
        var testSagaException = new TestSagaBase(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withMethod(testSagaException::sumAsConsumerWithException, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(
                testSagaModel.getBean()::sumAsConsumerWithException,
                testSagaModel.getBean()::diffForConsumerWithExceptionHandling,
                5
            )
            .start()
            .waitCompletion()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(100);
    }
}
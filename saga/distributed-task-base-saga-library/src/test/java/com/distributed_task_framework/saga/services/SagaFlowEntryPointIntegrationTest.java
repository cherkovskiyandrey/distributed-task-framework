package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import jakarta.annotation.Nullable;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaFlowEntryPointIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldRegisterToRun() {
        //when
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaBase(10));

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
        class TestSaga extends TestSagaBase {

            public TestSaga(int value) {
                super(value);
            }

            @Override
            public int sumAsFunction(int input) {
                super.sumAsFunction(input);
                throw new TestUserUncheckedException();
            }

            @Override
            public void diffForFunction(int input, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                super.diffForFunction(input, output, throwable);
            }
        }

        var testSagaException = new TestSaga(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withMethod(testSagaException::sumAsFunction, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sumAsFunction,
                testSagaModel.getBean()::diffForFunction,
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
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaBase(0));

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
        class TestSaga extends TestSagaBase {

            public TestSaga(int value) {
                super(value);
            }

            @Override
            public void sumAsConsumer(int input) {
                super.sumAsConsumer(input);
                throw new TestUserUncheckedException();
            }

            @Override
            public void diffForConsumer(int input, @Nullable SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                super.diffForConsumer(input, throwable);
            }
        }

        var testSagaException = new TestSaga(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withMethod(testSagaException::sumAsConsumer, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(
                testSagaModel.getBean()::sumAsConsumer,
                testSagaModel.getBean()::diffForConsumer,
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
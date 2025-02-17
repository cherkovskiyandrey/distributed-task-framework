package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaFlowBuilderWithoutInputImplIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldThenConsume() {
        //when
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaBase(0));

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(testSagaModel.getBean()::sumAsConsumer, 10)
            .thenConsume(testSagaModel.getBean()::multiplyAsConsumer)
            .start()
            .waitCompletion();

        //verify
        assertThat(testSagaModel.getBean().getValue()).isEqualTo(100);
    }

    @SneakyThrows
    @Test
    void shouldThenConsumeWhenWithRevert() {
        //when
        @Getter
        class TestSaga extends TestSagaBase {
            public TestSaga(int value) {
                super(value);
            }

            @Override
            public void multiplyAsConsumer(int input) {
                super.multiplyAsConsumer(input);
                throw new TestUserUncheckedException();
            }

            @Override
            public void divideForConsumer(int input, @Nullable SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                super.divideForConsumer(input, throwable);
            }
        }

        var testSagaException = new TestSaga(10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::multiplyAsConsumer, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(
                testSagaModel.getBean()::sumAsConsumer,
                testSagaModel.getBean()::diffForConsumer,
                5
            )
            .thenConsume(
                testSagaModel.getBean()::multiplyAsConsumer,
                testSagaModel.getBean()::divideForConsumer
            )
            .start()
            .waitCompletion()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(10);
    }

    @SneakyThrows
    @Test
    void shouldThenRun() {
        //when
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaBase(10));

        //do
        var resultOpt = distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(testSagaModel.getBean()::sumAsConsumer, 10)
            .thenRun(testSagaModel.getBean()::multiplyAsFunction)
            .start()
            .get();

        //verify
        assertThat(testSagaModel.getBean().getValue()).isEqualTo(200);
        assertThat(resultOpt)
            .isPresent()
            .get()
            .isEqualTo(200);
    }

    @SneakyThrows
    @Test
    void shouldThenRunWhenWithRevert() {
        @Getter
        class TestSaga extends TestSagaBase {

            public TestSaga(int value) {
                super(value);
            }

            @Override
            public int multiplyAsFunction(int input) {
                super.multiplyAsFunction(input);
                throw new TestUserUncheckedException();
            }

            @Override
            public void divideForFunction(int input, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                super.divideForFunction(input, output, throwable);
            }
        }
        var testSagaException = new TestSaga(10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::multiplyAsFunction, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(
                testSagaModel.getBean()::sumAsConsumer,
                testSagaModel.getBean()::diffForConsumer,
                5
            )
            .thenRun(
                testSagaModel.getBean()::multiplyAsFunction,
                testSagaModel.getBean()::divideForFunction
            )
            .start()
            .waitCompletion()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(10);
    }

    @SneakyThrows
    @Test
    void shouldThenRunWhenWithRevertInTheMiddle() {
        @Getter
        class TestSaga extends TestSagaBase {

            public TestSaga(int value) {
                super(value);
            }

            @Override
            public void divideForFunction(int input, Integer output, @Nullable SagaExecutionException throwable) {
                assertThat(output).isNotNull().isEqualTo(value);
                assertThat(throwable).isNull();
                super.divideForFunction(input, output, throwable);
            }
        }
        var testSagaException = new TestSaga(10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::justThrowExceptionAsConsumer, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(
                testSagaModel.getBean()::sumAsConsumer,
                testSagaModel.getBean()::diffForConsumer,
                5
            )
            .thenRun(
                testSagaModel.getBean()::multiplyAsFunction,
                testSagaModel.getBean()::divideForFunction
            )
            .thenConsume(testSagaModel.getBean()::justThrowExceptionAsConsumer)
            .start()
            .waitCompletion()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(10);
    }
}
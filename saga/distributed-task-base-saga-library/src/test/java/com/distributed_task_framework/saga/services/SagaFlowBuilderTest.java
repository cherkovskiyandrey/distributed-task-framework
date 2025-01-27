package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.Revert;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import jakarta.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class SagaFlowBuilderTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldDoThenRun() {
        //when
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaBase(10));

        //do
        var resultOpt = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
            .thenRun(testSagaModel.getBean()::multiplyAsFunction)
            .start()
            .get();

        //verify
        assertThat(resultOpt)
            .isPresent()
            .get()
            .isEqualTo(400);
    }

    @SneakyThrows
    @Test
    void shouldDoThenRunWhenWithRevert() {
        //when
        class TestSaga extends TestSagaBase {

            public TestSaga(int value) {
                super(value);
            }

            public int multiplyAsFunction(int parentOutput) {
                value *= parentOutput;
                throw new TestUserUncheckedException();
            }

            @Override
            public void divideForFunction(int parentOutput, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
                assertThat(output).isNull();
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                super.divideForFunction(parentOutput, output, throwable);
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
            .registerToRun(
                testSagaModel.getBean()::sumAsFunction,
                testSagaModel.getBean()::diffForFunction,
                10
            )
            .thenRun(
                testSagaModel.getBean()::multiplyAsFunction,
                testSagaModel.getBean()::divideForFunction
            )
            .start()
            .get()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(10);
    }

    @SneakyThrows
    @Test
    void shouldDoThenRunWhenWithRevertInTheMiddle() {
        //when
        class TestSaga extends TestSagaBase {

            public TestSaga(int value) {
                super(value);
            }

            @Override
            public void divideForFunction(int parentOutput, Integer output, @Nullable SagaExecutionException throwable) {
                assertThat(output).isNotNull().isEqualTo(value);
                assertThat(throwable).isNull();
                super.divideForFunction(parentOutput, output, throwable);
            }
        }

        var testSagaException = new TestSaga(10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::justThrowException, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sumAsFunction,
                testSagaModel.getBean()::diffForFunction,
                10
            )
            .thenRun(
                testSagaModel.getBean()::multiplyAsFunction,
                testSagaModel.getBean()::divideForFunction
            )
            .thenConsume(testSagaModel.getBean()::justThrowException)
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
    void shouldDoThenRunWhenWithRootInput() {
        //when
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaBase(10));

        //do
        var resultOpt = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
            .thenRun(testSagaModel.getBean()::multiplyAsFunctionWithRootInput)
            .start()
            .get();

        //verify
        assertThat(resultOpt)
            .isPresent()
            .get()
            .isEqualTo(4000);
    }

    @SneakyThrows
    @Test
    void shouldDoThenRunWhenWithRootInputAndRevert() {
        //when
        @Getter
        class TestSaga extends TestSagaBase {

            public TestSaga(int value) {
                super(value);
            }

            @Override
            public int multiplyAsFunctionWithRootInput(int parentOutput, int rootInput) {
                super.multiplyAsFunctionWithRootInput(parentOutput, rootInput);
                throw new TestUserUncheckedException();
            }

            @Override
            public void divideForFunctionWithRootInput(int parentOutput,
                                                       int rootInput,
                                                       @Nullable Integer output,
                                                       @Nullable SagaExecutionException throwable) {
                assertThat(output).isNull();
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                super.divideForFunctionWithRootInput(parentOutput, rootInput, output, throwable);
            }
        }

        var testSagaException = new TestSaga(10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::multiplyAsFunctionWithRootInput, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sumAsFunction,
                testSagaModel.getBean()::diffForFunction,
                10
            )
            .thenRun(
                testSagaModel.getBean()::multiplyAsFunctionWithRootInput,
                testSagaModel.getBean()::divideForFunctionWithRootInput
            )
            .start()
            .get()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(10);
    }

    @SneakyThrows
    @Test
    void shouldDoThenRunWhenWithRootInputAndRevertInTheMiddle() {
        //when
        @Getter
        class TestSaga extends TestSagaBase {

            public TestSaga(int value) {
                super(value);
            }

            @Override
            public void divideForFunctionWithRootInput(int parentOutput,
                                                       int rootInput,
                                                       @Nullable Integer output,
                                                       @Nullable SagaExecutionException throwable) {
                assertThat(output).isEqualTo(value);
                assertThat(throwable).isNull();
                super.divideForFunctionWithRootInput(parentOutput, rootInput, output, throwable);
            }
        }

        var testSagaException = new TestSaga(10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::justThrowException, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sumAsFunction,
                testSagaModel.getBean()::diffForFunction,
                10
            )
            .thenRun(
                testSagaModel.getBean()::multiplyAsFunctionWithRootInput,
                testSagaModel.getBean()::divideForFunctionWithRootInput
            )
            .thenConsume(testSagaModel.getBean()::justThrowException)
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
    void shouldThenConsume() {
        //when
        var testSaga = new TestSagaBase(10);
        var testSagaModel = testSagaGenerator.generateDefaultFor(testSaga);

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
            .thenConsume(testSagaModel.getBean()::multiplyAsConsumer)
            .start()
            .waitCompletion();

        //verify
        assertThat(testSaga.getValue()).isEqualTo(400);
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
            public void multiplyAsConsumer(int parentOutput) {
                super.multiplyAsConsumer(parentOutput);
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
            .registerToRun(
                testSagaModel.getBean()::sumAsFunction,
                testSagaModel.getBean()::diffForFunction,
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
    void shouldThenConsumeWhenWithRootInput() {
        //when
        var testSaga = new TestSagaBase(10);
        var testSagaModel = testSagaGenerator.generateDefaultFor(testSaga);

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sumAsFunction, 10)
            .thenConsume(testSagaModel.getBean()::multiplyAsConsumerWithRootInput)
            .start()
            .waitCompletion();

        //verify
        assertThat(testSaga.getValue()).isEqualTo(4000);
    }

    @Test
    void shouldThenConsumeWhenWithRootInputAndRevert() {
        //when
        @Getter
        class TestSaga extends TestSagaBase {

            public TestSaga(int value) {
                super(value);
            }

            @Override
            public void multiplyAsConsumerWithRootInput(int parentOutput, int input) {
                super.multiplyAsConsumerWithRootInput(parentOutput, input);
                throw new TestUserUncheckedException();
            }

            @Override
            public void divideForConsumerWithRootInput(int parentOutput,
                                                       int rootInput,
                                                       SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                super.divideForConsumerWithRootInput(parentOutput, rootInput, throwable);
            }
        }

        var testSagaException = new TestSaga(10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::multiplyAsConsumerWithRootInput, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sumAsFunction,
                testSagaModel.getBean()::diffForFunction,
                10
            )
            .thenConsume(
                testSagaModel.getBean()::multiplyAsConsumerWithRootInput,
                testSagaModel.getBean()::divideForConsumerWithRootInput
            )
            .start()
            .waitCompletion()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(10);
    }

    @Getter
    @AllArgsConstructor
    static class TestSagaBase {
        protected int value;

        public int sumAsFunction(int input) {
            value += input;
            return value;
        }

        @Revert
        public void diffForFunction(int input, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
            assertThat(throwable).isNull();
            assertThat(output).isNotNull().isEqualTo(value);
            value -= input;
        }

        public void multiplyAsConsumer(int parentOutput) {
            value *= parentOutput;
        }

        @Revert
        public void divideForConsumer(int parentOutput, @Nullable SagaExecutionException throwable) {
            value /= parentOutput;
        }

        public void multiplyAsConsumerWithRootInput(int parentOutput, int input) {
            value *= input * parentOutput;
        }

        @Revert
        public void divideForConsumerWithRootInput(int parentOutput,
                                                   int rootInput,
                                                   SagaExecutionException throwable) {
            value /= parentOutput * rootInput;
        }

        public int multiplyAsFunction(int parentOutput) {
            value *= parentOutput;
            return value;
        }

        @Revert
        public void divideForFunction(int parentOutput, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
            value /= parentOutput;
        }

        public int multiplyAsFunctionWithRootInput(int parentOutput, int rootInput) {
            value *= parentOutput * rootInput;
            return value;
        }

        @Revert
        public void divideForFunctionWithRootInput(int parentOutput,
                                                   int rootInput,
                                                   @Nullable Integer output,
                                                   @Nullable SagaExecutionException throwable) {
            value /= parentOutput * rootInput;
        }

        public void justThrowException(int input) {
            throw new TestUserUncheckedException();
        }
    }
}
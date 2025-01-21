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

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaFlowBuilderTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldDoThenRun() {
        record TestSaga(int value) {
            public int sum(int input) {
                return value + input;
            }

            public int doDouble(int parentOutput) {
                return parentOutput * 2;
            }
        }
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSaga(10));

        //do
        var resultOpt = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sum, 10)
            .thenRun(testSagaModel.getBean()::doDouble)
            .start()
            .get();

        //verify
        assertThat(resultOpt)
            .isPresent()
            .get()
            .isEqualTo(40);
    }

    @SneakyThrows
    @Test
    void shouldDoThenRunWhenWithRevert() {
        @Getter
        @AllArgsConstructor
        class TestSaga {
            private final int initial;
            private int value;

            public int sum(int input) {
                value += input;
                return value;
            }

            @Revert
            public void diff(int input, @Nullable Integer output, SagaExecutionException throwable) {
                assertThat(output).isNotNull().matches(i -> initial == i - input);
                value -= input;
            }

            public int doDouble(int parentOutput) {
                value = parentOutput * 2;
                throw new TestUserUncheckedException();
            }

            @Revert
            public void doDivideBy2(int parentOutput, @Nullable Integer output, SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                assertThat(output).isNull();
                value /= 2;
            }
        }

        var testSagaException = new TestSaga(10, 10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::doDouble, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff,
                5
            )
            .thenRun(
                testSagaModel.getBean()::doDouble,
                testSagaModel.getBean()::doDivideBy2
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
    void shouldDoThenRunWhenWithRootInput() {
        record TestSaga(int value) {
            public int sum(int input) {
                return value + input;
            }

            public int divide(int parentOutput, int rootInput) {
                return parentOutput / rootInput;
            }
        }
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSaga(10));

        //do
        var resultOpt = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sum, 10)
            .thenRun(testSagaModel.getBean()::divide)
            .start()
            .get();

        //verify
        assertThat(resultOpt)
            .isPresent()
            .get()
            .isEqualTo(2);
    }

    @SneakyThrows
    @Test
    void shouldDoThenRunWhenWithRootInputAndRevert() {
        //when
        @Getter
        @AllArgsConstructor
        class TestSaga {
            private final int initial;
            private int value;

            public int sum(int input) {
                value += input;
                return value;
            }

            @Revert
            public void diff(int input, @Nullable Integer output, SagaExecutionException throwable) {
                assertThat(throwable).isNull();
                assertThat(output).isNotNull().matches(i -> initial == i - input);
                value -= input;
            }

            public int doDivide(int parentOutput, int rootInput) {
                assertThat(parentOutput).isEqualTo(value);
                value = parentOutput / rootInput;
                throw new TestUserUncheckedException();
            }

            @Revert
            public void doMultiply(int parentOutput,
                                   int rootInput,
                                   @Nullable Integer output,
                                   SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                assertThat(output).isNull();
                value *= rootInput;
                assertThat(value).isEqualTo(parentOutput);
            }
        }

        var testSagaException = new TestSaga(10, 10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::doDivide, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff,
                10
            )
            .thenRun(
                testSagaModel.getBean()::doDivide,
                testSagaModel.getBean()::doMultiply
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
    void shouldThenConsume() {
        @Getter
        @AllArgsConstructor
        class TestSaga {
            int value;

            public int sum(int input) {
                return value + input;
            }

            public void doDouble(int parentOutput) {
                value = parentOutput * 2;
            }
        }
        var testSaga = new TestSaga(10);
        var testSagaModel = testSagaGenerator.generateDefaultFor(testSaga);

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sum, 10)
            .thenConsume(testSagaModel.getBean()::doDouble)
            .start()
            .waitCompletion();

        //verify
        assertThat(testSaga.getValue()).isEqualTo(40);
    }

    @SneakyThrows
    @Test
    void shouldThenConsumeWhenWithRevert() {
        @Getter
        @AllArgsConstructor
        class TestSaga {
            private final int initial;
            private int value;

            public int sum(int input) {
                value += input;
                return value;
            }

            @Revert
            public void diff(int input, @Nullable Integer output, SagaExecutionException throwable) {
                assertThat(output).isNotNull().matches(i -> initial == i - input);
                value -= input;
            }

            public void doDouble(int parentOutput) {
                value = parentOutput * 2;
                throw new TestUserUncheckedException();
            }

            @Revert
            public void doDivideBy2(int parentOutput, SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                value /= 2;
            }
        }

        var testSagaException = new TestSaga(10, 10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::doDouble, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff,
                5
            )
            .thenConsume(
                testSagaModel.getBean()::doDouble,
                testSagaModel.getBean()::doDivideBy2
            )
            .start()
            .waitCompletion()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(10);
    }

    @Test
    void testThenConsume1() {
        //todo
    }

    @Test
    void testThenConsume2() {
        //todo
    }

    //todo: 3 level hierarchy
    //todo: SagaNotFoundException when is too late
    //todo: TimeoutException when timeout expired
}
package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.Revert;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class DistributionSagaServiceIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldExecuteWhenOneSimpleMethodSaga() {
        //when
        record TestSaga(int delta) {
            public int sum(int i) {
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
        record TestSaga() {
            public int sum(int i) {
                return i + 10;
            }

            public int multiply(int i) {
                return i * 10;
            }

            public int divide(int i) {
                return i / 5;
            }
        }
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSaga());

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

    @Getter
    static class TestSagaException {
        private int value;

        public TestSagaException(int value) {
            this.value = value;
        }

        public int sum(int i) {
            value += i;
            throw new TestUserUncheckedException();
        }

        @Revert
        public void diff(int input, @Nullable Integer sumOutput, SagaExecutionException throwable) {
            assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
            value -= input;
        }
    }

    @SneakyThrows
    @Test
    void shouldExecuteRevertWhenException() {
        //when
        var testSagaException = new TestSagaException(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::sum, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff,
                5
            )
            .start()
            .waitCompletion();

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(100);
    }

    @SneakyThrows
    @Test
    void shouldExecuteRevertOnlyOnceWhenNoRetryFor() {
        //when
        var testSagaException = new TestSagaException(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(
                testSagaException::sum,
                TestSagaGeneratorUtils.withNoRetryFor(TestUserUncheckedException.class)
            )
            .build()
        );

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff,
                5
            )
            .start()
            .waitCompletion();

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(100);
    }

    //todo: consumer, biconsumer, function. bifucntion, etc
    //todo: sync/async/status/wait
    //todo: noRetryFor?
    //todo: cancelation

}
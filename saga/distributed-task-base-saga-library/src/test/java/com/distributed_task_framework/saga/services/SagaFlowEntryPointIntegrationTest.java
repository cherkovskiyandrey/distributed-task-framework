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
import lombok.Setter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaFlowEntryPointIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldRegisterToRun() {
        //when
        record TestSaga(int value) {
            public int sum(int i) {
                return value + i;
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
    void shouldRegisterToRunWhenWithRevert() {
        //when
        @Getter
        @AllArgsConstructor
        class TestSaga {
            private int value;

            public int sum(int inputArg) {
                value += inputArg;
                throw new TestUserUncheckedException();
            }

            @Revert
            public void diff(int inputArg, @Nullable Integer sumOutput, SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                value -= inputArg;
            }
        }

        var testSagaException = new TestSaga(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::sum, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff,
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
        @Getter
        @Setter
        @AllArgsConstructor
        class TestSagaToConsume {
            private int value;
        }
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSagaToConsume(0));

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(testSagaModel.getBean()::setValue, 100)
            .start()
            .waitCompletion();

        //verify
        assertThat(testSagaModel.getBean().getValue()).isEqualTo(100);
    }

    @SneakyThrows
    @Test
    void shouldRegisterToConsumeWhenWithRevert() {
        @Getter
        @AllArgsConstructor
        class TestSaga {
            private int value;

            public void sum(int inputArg) {
                value += inputArg;
                throw new TestUserUncheckedException();
            }

            @Revert
            public void diff(int inputArg, SagaExecutionException throwable) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
                value -= inputArg;
            }
        }

        var testSagaException = new TestSaga(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withRegisterAllMethods(true)
            .withMethod(testSagaException::sum, TestSagaGeneratorUtils.withoutRetry())
            .build()
        );

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToConsume(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff,
                5
            )
            .start()
            .waitCompletion();

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(100);
    }
}
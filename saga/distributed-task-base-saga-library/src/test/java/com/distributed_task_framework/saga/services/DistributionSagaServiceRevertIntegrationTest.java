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

//todo: think about actuality of this test
public class DistributionSagaServiceRevertIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldExecuteRevertFromRootWhenException() {
        //when
        var testSagaException = new TestSagaException(105, 100);
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
    void shouldExecuteRevertFromTheMiddleWhenException() {
        //when
        var testSagaException = new TestSagaException(40, 0);
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
            .thenRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff
            )
            .thenRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff
            )
            //revert from here
            .thenRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff
            )
            .thenRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff
            )
            .start()
            .waitCompletion();

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(0);
    }

    @Getter
    static class TestSagaException {
        private final int watermark;
        private int value;

        public TestSagaException(int watermark, int value) {
            this.watermark = watermark;
            this.value = value;
        }

        public int sum(int i) {
            value += i;
            if (isWatermark()) {
                throw new TestUserUncheckedException();
            }
            return value;
        }

        @Revert
        public void diff(int input, @Nullable Integer sumOutput, SagaExecutionException throwable) {
            if (isWatermark()) {
                assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
            }
            value -= input;
        }

        private boolean isWatermark() {
            return value == watermark;
        }
    }
}

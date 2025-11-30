package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.Revert;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils.withStopOnFailedAnyRevert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


//todo: serialisation of complex type
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
        assertThat(resultOpt).hasValue(20);
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

    private static Stream<Arguments> shouldHandleWhenFailedInRevertSupplier() {
        return Stream.of(
            Arguments.of(false, 390),
            Arguments.of(true, 400)
        );
    }
    @SneakyThrows
    @ParameterizedTest
    @MethodSource("shouldHandleWhenFailedInRevertSupplier")
    void shouldHandleWhenFailedInRevert(boolean stopOnFailedAnyRevert, int expectedValue) {
        //when
        var testSagaException = new TestSagaBase(10);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
            .withMethod(testSagaException::multiplyAsFunctionWithException, TestSagaGeneratorUtils.withoutRetry())
            .withSagaSettings(withStopOnFailedAnyRevert(stopOnFailedAnyRevert))
            .build()
        );

        //do
        assertThatThrownBy(() -> distributionSagaService.create(testSagaModel.getName())
            .registerToRun(
                testSagaModel.getBean()::sumAsFunction, //20
                testSagaModel.getBean()::diffForFunction,
                10
            )
            .thenRun(
                testSagaModel.getBean()::multiplyAsFunction, //400
                testSagaModel.getBean()::divideForFunctionWithException
            )
            .thenRun(
                testSagaModel.getBean()::multiplyAsFunctionWithException, //400*400
                testSagaModel.getBean()::divideForFunctionWithExceptionHandling
            )
            .start()
            .get()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

        //verify
        assertThat(testSagaException.getValue()).isEqualTo(expectedValue);
    }

    @SneakyThrows
    @ParameterizedTest
    @ValueSource(ints = {5, 10, 20, 40, 80})
    void shouldHandleLongSagaChainWhenException(int watermark) {
        //when
        var testSagaException = new TestSagaException(watermark, 0);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaException)
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
            .thenRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff
            )
            .thenRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff
            )
            .thenRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff
            )
            .thenRun(
                testSagaModel.getBean()::sum,
                testSagaModel.getBean()::diff
            )
            .start()
            .waitCompletion()
        )
            .isInstanceOf(SagaExecutionException.class)
            .hasCauseInstanceOf(TestUserUncheckedException.class);

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

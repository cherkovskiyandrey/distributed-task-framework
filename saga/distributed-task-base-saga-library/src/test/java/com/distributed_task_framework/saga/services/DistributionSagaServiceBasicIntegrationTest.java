package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.TestSagaGeneratorUtils;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


//todo: think about actuality of this test
class DistributionSagaServiceBasicIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldExecuteWhenOneSimpleMethodSaga() {
        //when
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSaga(100));

        //do
        var resultOpt = distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sum, 10)
            .start()
            .get(); //todo: move to

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
        var testSagaModel = testSagaGenerator.generateDefaultFor(new TestSaga(0));

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

    //todo: rewrite
    @SneakyThrows
    @Test
    void shouldNotRetryWhenNoRetryFor() {
        //when
        class TestSagaNotRetry extends TestSaga {
            public TestSagaNotRetry(int value) {
                super(value);
            }

            @Override
            public int sum(int i) {
                super.sum(i);
                throw new TestUserUncheckedException();
            }
        }

        var testSagaNoRetryFor = new TestSagaNotRetry(100);
        var testSagaModel = testSagaGenerator.generate(TestSagaModelSpec.builder(testSagaNoRetryFor)
            .withRegisterAllMethods(true)
            .withMethod(
                testSagaNoRetryFor::sum,
                TestSagaGeneratorUtils.withNoRetryFor(TestUserUncheckedException.class)
            )
            .build()
        );

        //do
        distributionSagaService.create(testSagaModel.getName())
            .registerToRun(testSagaModel.getBean()::sum, 5)
            .start()
            .waitCompletion();

        //verify
        assertThat(testSagaNoRetryFor.getValue()).isEqualTo(105);
    }


    @Getter
    public static class TestSaga {
        private int value;

        public TestSaga(int value) {
            this.value = value;
        }

        public int sum(int i) {
            value += i;
            return i;
        }

        public int diff(int i) {
            return value - i;
        }

        public int multiply(int i) {
            return i * 10;
        }

        public int divide(int i) {
            return i / 5;
        }
    }

    //todo: consumer, biconsumer, function. bifucntion, etc
    //todo: sync/async/status/wait
    //todo: cancelation
    //todo: saga expiration


}
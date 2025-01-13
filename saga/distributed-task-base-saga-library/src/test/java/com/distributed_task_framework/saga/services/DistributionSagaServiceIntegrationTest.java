package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

//todo
class DistributionSagaServiceIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldExecuteWhenOneSimpleMethodSaga() {
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
        //todo
    }
}
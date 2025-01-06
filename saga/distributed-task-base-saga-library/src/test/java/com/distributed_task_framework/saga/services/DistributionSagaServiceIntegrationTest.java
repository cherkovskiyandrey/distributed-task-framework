package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

//todo
class DistributionSagaServiceIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldExecuteSimpleSaga() {
        //when
        String sagaName = RandomStringUtils.random(10);
        String sagaMethodName = RandomStringUtils.random(10);

        record TestSaga(int delta) {
            private int sum(int i) {
                return i + delta;
            }
        }
        var testSaga = new TestSaga(10);

        distributionSagaService.registerSagaSettings(sagaName, SagaSettings.DEFAULT);
        distributionSagaService.registerSagaMethod(
            sagaMethodName,
            testSaga::sum,
            testSaga,
            SagaMethodSettings.DEFAULT
        );
        registeredSagas.add(sagaMethodName);

        //do
        var resultOpt = distributionSagaService.create(sagaName)
            .registerToRun(testSaga::sum, 10)
            .start()
            .get();

        //verify
        assertThat(resultOpt)
            .isPresent()
            .get()
            .isEqualTo(20);
    }

    //todo: check inherited methods
}
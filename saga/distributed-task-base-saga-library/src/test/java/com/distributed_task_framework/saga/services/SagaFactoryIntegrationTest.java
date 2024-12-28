package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

//todo
class SagaFactoryIntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldExecuteSimpleSaga() {
        //when

        //do
        var resultOpt = sagaFactory.create(RandomStringUtils.random(10))
            //todo: wrap to special bean
            .registerToRun(i -> i + 10, 10)
            .start()
            .get();

        //verify
        assertThat(resultOpt)
            .isPresent()
            .get()
            .isEqualTo(20);
    }
}
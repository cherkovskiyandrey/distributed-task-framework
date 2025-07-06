package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntegrationTest extends BaseSpringIntegrationTest {

    @SneakyThrows
    @Test
    void shouldRunWhenExternal() {
        //when
        externalSagaTestService.setValue("hello");

        //do
        assertThatThrownBy(() -> distributionSagaService.create("test")
            .registerToRun(
                externalSagaTestService::forward,
                externalSagaTestService::backward,
                "world"
            )
            .start()
            .get()
        ).isInstanceOf(SagaExecutionException.class);

        //verify
        assertThat(externalSagaTestService.getValue()).isEqualTo("hello+world-world");
    }

    @Test
    void shouldRunWhenInternal() {
        //when
        internalSagaTestService.setValue("hello");

        //do
        assertThatThrownBy(() -> internalSagaTestService.calculate("world"))
            .isInstanceOf(SagaExecutionException.class);

        //verify
        assertThat(internalSagaTestService.getValue()).isEqualTo("hello+world-world");
    }

    @Test
    void shouldDistinctWhenSagaSpecific() {
        //when
        sagaSpecificTestServiceOne.setValue("hello");
        sagaSpecificTestServiceTwo.setValue("crazy");

        //do
        assertThatThrownBy(() -> sagaSpecificTestServiceOne.calculate("world"))
            .isInstanceOf(SagaExecutionException.class);

        //verify
        assertThat(sagaSpecificTestServiceOne.getValue()).isEqualTo("hello+world-world");

        //do
        assertThatThrownBy(() -> sagaSpecificTestServiceTwo.calculate("people"))
            .isInstanceOf(SagaExecutionException.class);

        //verify
        assertThat(sagaSpecificTestServiceTwo.getValue()).isEqualTo("crazy+people-people");
    }
}
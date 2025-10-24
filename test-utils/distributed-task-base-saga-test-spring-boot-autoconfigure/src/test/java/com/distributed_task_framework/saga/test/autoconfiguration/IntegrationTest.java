package com.distributed_task_framework.saga.test.autoconfiguration;

import com.distributed_task_framework.Postgresql16Initializer;
import com.distributed_task_framework.Signaller;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.test.autoconfiguration.service.SagaTestUtil;
import com.distributed_task_framework.test.autoconfigure.TestDistributedTaskAutoconfiguration;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.PlatformTransactionManager;

//TODO: add metrics !!!
@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.saga.enabled=true",
        "distributed-task.common.app-name=test"
    }
)
@EnableAutoConfiguration
@ContextConfiguration(
    classes = {
        AdditionalTestConfiguration.class,
        TestDistributedTaskAutoconfiguration.class,
        TestSagaAutoconfiguration.class
    },
    initializers = Postgresql16Initializer.class
)
@FieldDefaults(level = AccessLevel.PRIVATE)
class IntegrationTest {
    @Autowired
    SagaRepository sagaRepository;
    @Autowired
    DistributionSagaService distributionSagaService;
    @Autowired
    SagaTestUtil sagaTestUtil;
    @Autowired
    PlatformTransactionManager transactionManager;
    @Autowired
    Signaller signaller;

    //todo
    @SneakyThrows
    @Test
    void shouldCancelAllSagasCorrectly() {
        //when

        //do

        //verify
    }
}
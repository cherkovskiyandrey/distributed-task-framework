package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.Postgresql16Initializer;
import com.distributed_task_framework.saga.autoconfigure.services.ExternalSagaTestService;
import com.distributed_task_framework.saga.autoconfigure.services.InternalSagaTestService;
import com.distributed_task_framework.saga.autoconfigure.services.SagaSpecificTestService;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.saga.enabled=true",
        "distributed-task.common.app-name=test",
        "distributed-task.saga.saga-method-properties-group.default-saga-method-properties.retry.retry-mode=OFF"
    }
)
@EnableAutoConfiguration
@ContextConfiguration(
    classes = {SagaAutoconfiguration.class, MappersConfiguration.class, AdditionalTestConfiguration.class},
    initializers = Postgresql16Initializer.class
)
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class BaseSpringIntegrationTest {
    @Autowired
    DistributedTaskTestUtil distributedTaskTestUtil;
    @Autowired
    DistributionSagaService distributionSagaService;
    @Autowired
    ExternalSagaTestService externalSagaTestService;
    @Autowired
    InternalSagaTestService internalSagaTestService;
    @Autowired
    @Qualifier("sagaSpecificTestServiceOne")
    SagaSpecificTestService sagaSpecificTestServiceOne;
    @Autowired
    @Qualifier("sagaSpecificTestServiceTwo")
    SagaSpecificTestService sagaSpecificTestServiceTwo;

    @SneakyThrows
    @BeforeEach
    @AfterEach
    public void init() {
        Assertions.setMaxStackTraceElementsDisplayed(100);
        distributedTaskTestUtil.reinitAndWait();
    }
}

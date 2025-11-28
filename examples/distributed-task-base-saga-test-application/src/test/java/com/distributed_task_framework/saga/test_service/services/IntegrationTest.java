package com.distributed_task_framework.saga.test_service.services;


import com.distributed_task_framework.saga.test.autoconfiguration.service.SagaTestUtil;
import com.distributed_task_framework.utils.Postgresql16Initializer;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.common.app-name=test",
        "distributed-task.saga.enabled=true"
    }
)
@ContextConfiguration(
    initializers = Postgresql16Initializer.class
)
@FieldDefaults(level = AccessLevel.PRIVATE)
class IntegrationTest {

    @Autowired
    SagaTestUtil sagaTestUtil;

    @Test
    @SneakyThrows
    void simpleTest() {
        sagaTestUtil.reinitAndWait();
    }
}
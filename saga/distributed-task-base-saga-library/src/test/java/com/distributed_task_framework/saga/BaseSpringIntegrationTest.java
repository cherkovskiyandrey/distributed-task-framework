package com.distributed_task_framework.saga;

import com.distributed_task_framework.Postgresql16Initializer;
import com.distributed_task_framework.saga.services.SagaFactory;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

@Disabled
@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.common.app-name=saga-test"
    }
)
@EnableAutoConfiguration
@ContextConfiguration(
    initializers = {Postgresql16Initializer.class},
    classes = {BaseTestConfiguration.class}
)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class BaseSpringIntegrationTest {
    @Autowired
    DistributedTaskTestUtil distributedTaskTestUtil;
    @Autowired
    SagaFactory sagaFactory;

    @SneakyThrows
    @BeforeEach
    public void init() {
        distributedTaskTestUtil.reinitAndWait();
    }
}

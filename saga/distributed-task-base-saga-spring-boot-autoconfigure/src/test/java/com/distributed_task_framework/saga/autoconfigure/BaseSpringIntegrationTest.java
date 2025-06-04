package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.Postgresql16Initializer;
import com.distributed_task_framework.saga.autoconfigure.SagaAutoconfiguration;
import com.distributed_task_framework.saga.autoconfigure.utils.MappersConfiguration;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

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
    classes = {SagaAutoconfiguration.class, MappersConfiguration.class},
    initializers = Postgresql16Initializer.class
)
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class BaseSpringIntegrationTest {
}

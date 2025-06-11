package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.service.TaskSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.common.app-name=test"
    })
@ContextConfiguration(classes = {
    TaskSerializerConfiguration.class,
    DataSourceAutoConfiguration.class,
    JdbcTemplateAutoConfiguration.class,
    DistributedTaskAutoconfigure.class
})
@EnableAutoConfiguration
public class TaskSerializerTest {
    @Autowired
    TaskSerializer taskSerializer;

    @Test
    void shouldHaveKotlinModule() {
        ObjectMapper objectMapper = (ObjectMapper) ReflectionTestUtils.getField(taskSerializer, "objectMapper");
        Assertions.assertThat(objectMapper.getRegisteredModuleIds())
            .contains("com.fasterxml.jackson.module.kotlin.KotlinModule");
    }
}

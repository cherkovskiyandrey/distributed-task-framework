package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.tasks.TestCreationInterceptor;
import com.distributed_task_framework.autoconfigure.tasks.TestCreationInterceptorTwo;
import com.distributed_task_framework.autoconfigure.tasks.TestExecutionInterceptor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DistributedTaskPropertiesBindingTest {

    @Test
    void shouldBindInterceptorClasses() {
        //when
        Binder binder = new Binder(new MapConfigurationPropertySource(Map.of(
            "distributed-task.task-properties-group.default-properties.creation-interceptors[0]",
                "com.distributed_task_framework.autoconfigure.tasks.TestCreationInterceptor",
            "distributed-task.task-properties-group.default-properties.creation-interceptors[1]",
                "com.distributed_task_framework.autoconfigure.tasks.TestCreationInterceptorTwo",
            "distributed-task.task-properties-group.default-properties.execution-interceptors[0]",
                "com.distributed_task_framework.autoconfigure.tasks.TestExecutionInterceptor"
        )));

        //do
        DistributedTaskProperties properties = binder
            .bind("distributed-task", Bindable.of(DistributedTaskProperties.class))
            .orElseThrow(IllegalStateException::new);

        //verify
        assertThat(properties.getTaskPropertiesGroup().getDefaultProperties().getCreationInterceptors())
            .containsExactly(TestCreationInterceptor.class, TestCreationInterceptorTwo.class);
        assertThat(properties.getTaskPropertiesGroup().getDefaultProperties().getExecutionInterceptors())
            .containsExactly(TestExecutionInterceptor.class);
    }
}

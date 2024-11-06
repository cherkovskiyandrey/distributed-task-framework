package com.distributed_task_framework.test.autoconfigure;

import com.distributed_task_framework.Postgresql16Initializer;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.test.autoconfigure.tasks.DefaultTask;
import com.distributed_task_framework.test.autoconfigure.tasks.SimpleCronCustomizedTask;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import static com.distributed_task_framework.test.ClusterProviderTestImpl.TEST_NODE_ID;
import static com.distributed_task_framework.test.autoconfigure.tasks.DefaultTask.TASK_DEF;
import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.common.app-name=test"
    })
@EnableAutoConfiguration
@ContextConfiguration(
    classes = TestDistributedTaskAutoconfiguration.class,
    initializers = Postgresql16Initializer.class
)
@ComponentScan(basePackageClasses = DefaultTask.class)
@FieldDefaults(level = AccessLevel.PRIVATE)
class TestDistributedTaskAutoconfigurationTest {
    @Autowired
    ClusterProvider clusterProvider;
    @Autowired
    DistributedTaskService distributedTaskService;

    @Test
    void shouldLoadCustomClusterProvider() {
        assertThat(clusterProvider.nodeId()).isEqualTo(TEST_NODE_ID);
    }

    @Test
    void shouldLoadRealTaskRegistryService() {
        assertThat(distributedTaskService.isTaskRegistered(TASK_DEF)).isTrue();
    }

    @Test
    void shouldNotScheduleCronTask() {
        assertThat(distributedTaskService.getRegisteredTask(SimpleCronCustomizedTask.TASK_DEF))
            .isNotEmpty()
            .get()
            .matches(registeredTask -> !registeredTask.getTaskSettings().hasCron(), "cron");
    }
}
package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.persistence.entity.RegisteredTaskEntity;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.task.TaskGenerator;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class TaskRegistryServiceIntegrationTest extends BaseSpringIntegrationTest {
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    TaskRegistryServiceImpl taskRegistryService;

    @Test
    void shouldExposeEntityWhenRegisterLocalTask() {
        //when
        Task<String> task = TaskGenerator.defineTask(
                TaskDef.privateTaskDef("task", String.class),
                m -> System.out.println("hello world!")
        );
        Collection<RegisteredTaskEntity> registeredTasks = registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId());
        assertThat(registeredTasks).isEmpty();

        //do
        taskRegistryService.registerTask(task, defaultTaskSettings);

        //verify
        registeredTasks = waitAndGet(
                () -> registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId()),
                regTasks -> !regTasks.isEmpty()
        );
        assertThat(registeredTasks).singleElement()
                .matches(taskEntity -> "task".equals(taskEntity.getTaskName()), "task name");
    }

    @Test
    void shouldNotExposeEntityWhenRegisterRemoteTasks() {
        //when
        Task<String> localTask = TaskGenerator.defineTask(
                TaskDef.privateTaskDef("task", String.class),
                m -> System.out.println("hello world!")
        );
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("foreign-app", "task", String.class);
        Collection<RegisteredTaskEntity> registeredTasks = registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId());
        assertThat(registeredTasks).isEmpty();

        //do
        taskRegistryService.registerTask(localTask, defaultTaskSettings);
        taskRegistryService.registerRemoteTask(remoteTaskDef, defaultTaskSettings);

        //verify
        registeredTasks = waitAndGet(
                () -> registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId()),
                regTasks -> !regTasks.isEmpty()
        );
        assertThat(registeredTasks).singleElement()
                .matches(taskEntity -> "task".equals(taskEntity.getTaskName()), "task name");
    }

    @Test
    void shouldDeleteEntityWhenExistedLocalTaskIsUnregistered() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("task", String.class);
        Task<String> task = TaskGenerator.defineTask(
                taskDef,
                m -> System.out.println("hello world!")
        );
        taskRegistryService.registerTask(task, defaultTaskSettings);
        waitAndGet(
                () -> registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId()),
                regTasks -> !regTasks.isEmpty()
        );

        //do
        taskRegistryService.unregisterLocalTask(taskDef.getTaskName());

        //verify
        waitAndGet(
                () -> registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId()),
                Collection::isEmpty
        );
    }

    @Test
    void shouldNotUpdateRegisteredTasksWhenTheyHaveNotBeenChanged() {
        //when
        Task<String> task1 = TaskGenerator.emptyDefineTask(TaskDef.privateTaskDef("task-1", String.class));
        Task<String> task2 = TaskGenerator.emptyDefineTask(TaskDef.privateTaskDef("task-2", String.class));
        Task<String> task3 = TaskGenerator.emptyDefineTask(TaskDef.privateTaskDef("task-3", String.class));

        var registeredTasks = registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId());
        Assertions.assertThat(registeredTasks).isEmpty();

        taskRegistryService.registerTask(task1, defaultTaskSettings);
        taskRegistryService.registerTask(task2, defaultTaskSettings);
        taskRegistryService.registerTask(task3, defaultTaskSettings);

        registeredTasks = waitAndGet(
                () -> registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId()),
                regTasks -> regTasks.size() == 3
        );

        //do
        taskRegistryService.publishOrUpdateTasksInCluster();

        //verify
        var registeredTasksAfterUpdating = registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId());
        Assertions.assertThat(registeredTasksAfterUpdating)
                .map(RegisteredTaskEntity::getId)
                .containsExactlyInAnyOrderElementsOf(registeredTasks.stream().map(RegisteredTaskEntity::getId).toList());
    }
}

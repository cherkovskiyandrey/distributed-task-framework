package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;
import com.distributed_task_framework.persistence.entity.RegisteredTaskEntity;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.utils.TaskGenerator;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertThat(registeredTasksAfterUpdating)
            .map(RegisteredTaskEntity::getId)
            .containsExactlyInAnyOrderElementsOf(registeredTasks.stream().map(RegisteredTaskEntity::getId).toList());
    }

    @Test
    void shouldReturnRegisteredLocalTaskInCluster() {
        //when
        var nodeId1 = UUID.randomUUID();
        var nodeId2 = UUID.randomUUID();
        var nodeId3 = UUID.randomUUID();
        var task1 = RegisteredTaskEntity.builder().taskName("task-1").nodeStateId(nodeId1).build();
        var task2 = RegisteredTaskEntity.builder().taskName("task-2").nodeStateId(nodeId2).build();
        var task3 = RegisteredTaskEntity.builder().taskName("task-3").nodeStateId(nodeId3).build();

        //do
        nodeStateRepository.saveAll(List.of(
                NodeStateEntity.builder()
                    .node(nodeId1)
                    .medianCpuLoading(0D)
                    .lastUpdateDateUtc(LocalDateTime.now(clock))
                    .build(),
                NodeStateEntity.builder()
                    .node(nodeId2)
                    .medianCpuLoading(0D)
                    .lastUpdateDateUtc(LocalDateTime.now(clock))
                    .build(),
                NodeStateEntity.builder()
                    .node(nodeId3)
                    .medianCpuLoading(0D)
                    .lastUpdateDateUtc(LocalDateTime.now(clock))
                    .build()
            )
        );
        registeredTaskRepository.saveOrUpdateBatch(List.of(task1, task2, task3));

        //verify
        var registeredTasks = waitAndGet(
            () -> taskRegistryService.getRegisteredLocalTaskInCluster(),
            regTasks -> regTasks.size() == 3
        );
        assertThat(registeredTasks).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                task1.getNodeStateId(), Set.of(task1.getTaskName()),
                task2.getNodeStateId(), Set.of(task2.getTaskName()),
                task3.getNodeStateId(), Set.of(task3.getTaskName())
            )
        );
    }

    @Test
    void shouldCheckHasClusterRegisteredTaskByName() {
        //when
        Task<String> ownTask = TaskGenerator.emptyDefineTask(TaskDef.privateTaskDef("ownTask", String.class));
        taskRegistryService.registerTask(ownTask, defaultTaskSettings);
        waitAndGet(
            () -> registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId()),
            regTasks -> !regTasks.isEmpty()
        );

        var externalNode = UUID.randomUUID();
        var externalTask = RegisteredTaskEntity.builder().taskName("externalTask").nodeStateId(externalNode).build();
        nodeStateRepository.saveAll(List.of(
                NodeStateEntity.builder()
                    .node(externalNode)
                    .medianCpuLoading(0D)
                    .lastUpdateDateUtc(LocalDateTime.now(clock))
                    .build()
            )
        );
        registeredTaskRepository.saveOrUpdateBatch(List.of(externalTask));

        //verify
        assertTrue(taskRegistryService.hasClusterRegisteredTaskByName(ownTask.getDef().getTaskName()));
        assertTrue(taskRegistryService.hasClusterRegisteredTaskByName(externalTask.getTaskName()));
        assertFalse(taskRegistryService.hasClusterRegisteredTaskByName("unknown-task-name"));
    }
}

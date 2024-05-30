package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractNestedLocalScheduleTest extends BaseLocalWorkerIntegrationTest {

    @Test
    void shouldScheduleNewTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            TaskId innerTask = distributedTaskService.schedule(taskDef, ExecutionContext.simple("childTaskOne"));
            assertThat(taskRepository.find(innerTask.getId())).isEmpty();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        assertThat(taskRepository.findByName("test", 2)).singleElement()
                .matches(te -> te.getVersion() == 1, "opt locking")
                .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "schedule time")
                .matches(te -> te.getAssignedWorker() == null, "free assigned worker");
    }

    @Test
    void shouldScheduleImmediatelyNewTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            TaskId innerTask = distributedTaskService.scheduleImmediately(taskDef, ExecutionContext.simple("childTaskOne"));
            assertThat(taskRepository.find(innerTask.getId())).isPresent();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        assertThat(taskRepository.findByName("test", 2)).singleElement()
                .matches(te -> te.getVersion() == 1, "opt locking")
                .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "schedule time")
                .matches(te -> te.getAssignedWorker() == null, "free assigned worker");
    }
}

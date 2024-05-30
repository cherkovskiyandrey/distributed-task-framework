package com.distributed_task_framework.service.impl.workers.remote;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.task.common.RemoteStubTask;
import com.distributed_task_framework.service.impl.remote_commands.CancelTaskByTaskDefCommand;
import com.distributed_task_framework.service.impl.remote_commands.CancelTaskCommand;
import com.distributed_task_framework.service.impl.remote_commands.RescheduleByTaskDefCommand;
import com.distributed_task_framework.service.impl.remote_commands.RescheduleCommand;
import com.distributed_task_framework.service.impl.remote_commands.ScheduleCommand;
import com.distributed_task_framework.service.impl.workers.local.BaseLocalWorkerIntegrationTest;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

//todo: when feature of supporting of remote command is in developing,
// should be split to classes as for local workers did.
@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractNestedRemoteCommandIntegrationTests extends BaseLocalWorkerIntegrationTest {
    @Autowired
    RemoteCommandRepository remoteCommandRepository;


    //----------- Schedule ------------
    @Test
    void shouldScheduleNewTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.schedule(remoteTaskDef, ExecutionContext.simple("childTaskOne"));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).singleElement()
                .matches(rce -> ScheduleCommand.NAME.equals(rce.getAction()), "command name")
                .matches(rce -> "remote".equals(rce.getAppName()), "app name")
                .matches(rce -> "remote-task".equals(rce.getTaskName()), "task name")
                .satisfies(rce -> Assertions.assertThat(readAsCommand(rce.getBody(), ScheduleCommand.class))
                        .matches(cmd -> remoteTaskDef.equals(cmd.getTaskDef()), "task def")
                        .matches(cmd -> "childTaskOne".equals(cmd.getExecutionContext().getInputMessageOrThrow()), "input message")
                )
        ;
    }

    @Test
    void shouldNotScheduleNewTaskWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.schedule(remoteTaskDef, ExecutionContext.simple("childTaskOne"));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).isEmpty();
    }

    @Test
    void shouldScheduleImmediatelyNewTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.scheduleImmediately(remoteTaskDef, ExecutionContext.simple("childTaskOne"));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    @Test
    void shouldScheduleImmediatelyNewTaskWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.scheduleImmediately(remoteTaskDef, ExecutionContext.simple("childTaskOne"));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    //----------- Reschedule ---------
    @Test
    void shouldRescheduleTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        TaskId remoteTaskId = TaskId.builder()
                .taskName("remote-task")
                .appName("remote")
                .id(UUID.randomUUID())
                .build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.reschedule(remoteTaskId, Duration.ofSeconds(60));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).singleElement()
                .matches(rce -> RescheduleCommand.NAME.equals(rce.getAction()), "command name")
                .matches(rce -> "remote".equals(rce.getAppName()), "app name")
                .matches(rce -> "remote-task".equals(rce.getTaskName()), "task name")
                .satisfies(rce -> Assertions.assertThat(readAsCommand(rce.getBody(), RescheduleCommand.class))
                        .matches(cmd -> remoteTaskId.equals(cmd.getTaskId()), "task id")
                        .matches(cmd -> Duration.ofSeconds(60).equals(cmd.getDelay()), "delay")
                )
        ;
    }

    @Test
    void shouldNotRescheduleTaskWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        TaskId remoteTaskId = TaskId.builder()
                .taskName("remote-task")
                .appName("remote")
                .id(UUID.randomUUID())
                .build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.reschedule(remoteTaskId, Duration.ofSeconds(60));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).isEmpty();
    }

    @Test
    void shouldRescheduleImmediatelyTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        TaskId remoteTaskId = TaskId.builder()
                .taskName("remote-task")
                .appName("remote")
                .id(UUID.randomUUID())
                .build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleImmediately(remoteTaskId, Duration.ofSeconds(60));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    @Test
    void shouldRescheduleImmediatelyTaskWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        TaskId remoteTaskId = TaskId.builder()
                .taskName("remote-task")
                .appName("remote")
                .id(UUID.randomUUID())
                .build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleImmediately(remoteTaskId, Duration.ofSeconds(60));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    //----------- Cancel ----------------------
    @Test
    void shouldCancelTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        TaskId remoteTaskId = TaskId.builder()
                .taskName("remote-task")
                .appName("remote")
                .id(UUID.randomUUID())
                .build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecution(remoteTaskId);
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).singleElement()
                .matches(rce -> CancelTaskCommand.NAME.equals(rce.getAction()), "command name")
                .matches(rce -> "remote".equals(rce.getAppName()), "app name")
                .matches(rce -> "remote-task".equals(rce.getTaskName()), "task name")
                .satisfies(rce -> Assertions.assertThat(readAsCommand(rce.getBody(), CancelTaskCommand.class))
                        .matches(cmd -> remoteTaskId.equals(cmd.getTaskId()), "task id")
                )
        ;
    }

    @Test
    void shouldNotCancelTaskWhenException() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        TaskId remoteTaskId = TaskId.builder()
                .taskName("remote-task")
                .appName("remote")
                .id(UUID.randomUUID())
                .build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecution(remoteTaskId);
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
    }

    @Test
    void shouldCancelTaskImmediately() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        TaskId remoteTaskId = TaskId.builder()
                .taskName("remote-task")
                .appName("remote")
                .id(UUID.randomUUID())
                .build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecutionImmediately(remoteTaskId);
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    @Test
    void shouldCancelTaskImmediatelyWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        TaskId remoteTaskId = TaskId.builder()
                .taskName("remote-task")
                .appName("remote")
                .id(UUID.randomUUID())
                .build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecutionImmediately(remoteTaskId);
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    //-------- RescheduleByTaskDef ---------------
    @Test
    void shouldRescheduleByTaskDef() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleByTaskDef(remoteTaskDef, Duration.ofSeconds(60));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).singleElement()
                .matches(rce -> RescheduleByTaskDefCommand.NAME.equals(rce.getAction()), "command name")
                .matches(rce -> "remote".equals(rce.getAppName()), "app name")
                .matches(rce -> "remote-task".equals(rce.getTaskName()), "task name")
                .satisfies(rce -> Assertions.assertThat(readAsCommand(rce.getBody(), RescheduleByTaskDefCommand.class))
                        .matches(cmd -> remoteTaskDef.equals(cmd.getTaskDef()), "remote task def")
                )
        ;
    }

    @Test
    void shouldNotRescheduleByTaskDefWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleByTaskDef(remoteTaskDef, Duration.ofSeconds(60));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).isEmpty();
    }

    @Test
    void shouldRescheduleByTaskDefImmediately() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleByTaskDefImmediately(remoteTaskDef, Duration.ofSeconds(60));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    @Test
    void shouldRescheduleByTaskDefImmediatelyWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleByTaskDefImmediately(remoteTaskDef, Duration.ofSeconds(60));
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    //--------- CancelAllTaskByTaskDef -------------
    @Test
    void shouldCancelByTaskDef() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelAllTaskByTaskDef(remoteTaskDef);
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).singleElement()
                .matches(rce -> CancelTaskByTaskDefCommand.NAME.equals(rce.getAction()), "command name")
                .matches(rce -> "remote".equals(rce.getAppName()), "app name")
                .matches(rce -> "remote-task".equals(rce.getTaskName()), "task name")
                .satisfies(rce -> Assertions.assertThat(readAsCommand(rce.getBody(), CancelTaskByTaskDefCommand.class))
                        .matches(cmd -> remoteTaskDef.equals(cmd.getTaskDef()), "remote task def")
                )
        ;
    }

    @Test
    void shouldNotCancelByTaskDefWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelAllTaskByTaskDef(remoteTaskDef);
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(0);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).isEmpty();
    }

    @Test
    void shouldCancelByTaskDefImmediately() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelAllTaskByTaskDefImmediately(remoteTaskDef);
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    @Test
    void shouldCancelByTaskDefImmediatelyWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef("remote", "remote-task", String.class);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelAllTaskByTaskDefImmediately(remoteTaskDef);
            Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
            throw new RuntimeException();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test")))
                .thenReturn(Optional.of(registeredTask));

        RemoteStubTask<String> stringRemoteStubTask = RemoteStubTask.stubFor(remoteTaskDef);
        RegisteredTask<String> registeredRemoteTask = RegisteredTask.of(stringRemoteStubTask, taskSettings);
        when(taskRegistryService.getRegisteredTask(eq(remoteTaskDef))).thenReturn(Optional.of(registeredRemoteTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent();
        Assertions.assertThat(remoteCommandRepository.findAll()).hasSize(1);
    }

    @SneakyThrows
    private <T> T readAsCommand(byte[] body, Class<T> cls) {
        return taskSerializer.readValue(body, cls);
    }
}

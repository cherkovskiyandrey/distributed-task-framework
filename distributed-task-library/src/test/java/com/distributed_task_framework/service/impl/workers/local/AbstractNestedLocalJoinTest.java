package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.JoinTaskMessageContainer;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import com.distributed_task_framework.utils.JdbcTools;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.lang.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractNestedLocalJoinTest extends BaseLocalWorkerIntegrationTest {

    @SneakyThrows
    @Test
    void shouldMoveOutputLinksToJoinTasksOnlyToLeavesTasksWhenChildrenNotForked() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        AtomicReference<TaskId> joinTaskId10Ref = new AtomicReference<>();
        AtomicReference<TaskId> joinTaskId11Ref = new AtomicReference<>();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            TaskId taskId1 = distributedTaskService.schedule(taskDef, m.withNewMessage("general-1"));
            TaskId taskId2 = distributedTaskService.schedule(taskDef, m.withNewMessage("general-2"));
            TaskId taskId3 = distributedTaskService.schedule(taskDef, m.withNewMessage("general-3"));
            TaskId taskId4 = distributedTaskService.schedule(taskDef, m.withNewMessage("general-4"));
            TaskId taskId5 = distributedTaskService.schedule(taskDef, m.withNewMessage("general-5"));

            TaskId joinTaskId6 = distributedTaskService.scheduleJoin(taskDef, m.withNewMessage("join-6"), List.of(taskId1, taskId2));
            TaskId joinTaskId7 = distributedTaskService.scheduleJoin(taskDef, m.withNewMessage("join-7"), List.of(taskId3, taskId4));
            TaskId joinTaskId8 = distributedTaskService.scheduleJoin(taskDef, m.withNewMessage("join-8"), List.of(taskId4, taskId5));
            TaskId joinTaskId9 = distributedTaskService.scheduleJoin(taskDef, m.withNewMessage("join-9"), List.of(joinTaskId6, taskId3));

            TaskId joinTaskId10 = distributedTaskService.scheduleJoin(taskDef, m.withNewMessage("join-10"), List.of(joinTaskId8));
            joinTaskId10Ref.set(joinTaskId10);
            TaskId joinTaskId11 = distributedTaskService.scheduleJoin(taskDef, m.withNewMessage("join-11"), List.of(joinTaskId9, joinTaskId7));
            joinTaskId11Ref.set(joinTaskId11);
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        TaskEntity parentTaskEntity = saveNewTaskEntity();
        TaskEntity joinTaskEntity1 = saveNewTaskEntity().toBuilder().notToPlan(true).build();
        TaskEntity joinTaskEntity2 = saveNewTaskEntity().toBuilder().notToPlan(true).build();

        TaskId parentTaskId = taskMapper.map(parentTaskEntity, commonSettings.getAppName());
        TaskId joinTaskId1 = taskMapper.map(joinTaskEntity1, commonSettings.getAppName());
        TaskId joinTaskId2 = taskMapper.map(joinTaskEntity2, commonSettings.getAppName());

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTaskId1, parentTaskId),
                        toJoinTaskLink(joinTaskId2, parentTaskId)
                )
        );

        //do
        getTaskWorker().execute(parentTaskEntity, registeredTask);

        //verify
        assertThat(joinTaskId10Ref.get()).isNotNull();
        assertThat(joinTaskId11Ref.get()).isNotNull();
        assertThat(taskRepository.find(parentTaskEntity.getId())).isEmpty();
        assertThat(taskLinkRepository.filterIntermediateTasks(JdbcTools.UUIDsToStringArray(List.of(parentTaskId.getId()))))
                .isEmpty();
        assertThat(taskLinkRepository.findAllByJoinTaskIdIn(List.of(joinTaskId1.getId())))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactlyInAnyOrder(
                        toJoinTaskLink(joinTaskId1, joinTaskId10Ref.get()),
                        toJoinTaskLink(joinTaskId1, joinTaskId11Ref.get())
                );
        assertThat(taskLinkRepository.findAllByJoinTaskIdIn(List.of(joinTaskId2.getId())))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactlyInAnyOrder(
                        toJoinTaskLink(joinTaskId2, joinTaskId10Ref.get()),
                        toJoinTaskLink(joinTaskId2, joinTaskId11Ref.get())
                );
    }

    @SneakyThrows
    @Test
    void shouldNotScheduleCronJoinTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskDef<String> cronTaskDef = TaskDef.privateTaskDef("cron-task", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskSettings recurrentTaskSettings = newRecurrentTaskSettings();

        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            TaskId taskId = distributedTaskService.schedule(taskDef, m.withNewMessage("general"));

            //verify
            assertThatThrownBy(() -> distributedTaskService.scheduleJoin(cronTaskDef, m.withNewMessage("join"), List.of(taskId)))
                    .isInstanceOf(TaskConfigurationException.class);
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        RegisteredTask<String> registeredCronTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(cronTaskDef), recurrentTaskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("cron-task"))).thenReturn(Optional.of(registeredCronTask));

        TaskEntity parentTaskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(parentTaskEntity, registeredTask);
    }

    @SneakyThrows
    @Test
    void shouldNotScheduleJoinTaskWhenJoinToCronTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskDef<String> cronTaskDef = TaskDef.privateTaskDef("cron-task", String.class);
        TaskDef<String> joinTaskDef = TaskDef.privateTaskDef("join-task", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskSettings recurrentTaskSettings = newRecurrentTaskSettings();

        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            TaskId taskId = distributedTaskService.schedule(cronTaskDef, m.withNewMessage("general"));

            //verify
            assertThatThrownBy(() -> distributedTaskService.scheduleJoin(taskDef, m.withNewMessage("join"), List.of(taskId)))
                    .isInstanceOf(TaskConfigurationException.class);
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        RegisteredTask<String> registeredCronTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(cronTaskDef), recurrentTaskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("cron-task"))).thenReturn(Optional.of(registeredCronTask));

        RegisteredTask<String> registeredJoinTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(joinTaskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("join-task"))).thenReturn(Optional.of(registeredJoinTask));

        TaskEntity parentTaskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(parentTaskEntity, registeredTask);
    }

    @SneakyThrows
    @Test
    void shouldMarkOutputLinksToJoinTasksAsCompletedWhenNotChildren() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
        });
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        TaskEntity parentTaskEntity = saveNewTaskEntity();
        TaskEntity joinTaskEntity1 = saveNewTaskEntity().toBuilder().notToPlan(true).build();
        TaskEntity joinTaskEntity2 = saveNewTaskEntity().toBuilder().notToPlan(true).build();

        TaskId parentTaskId = taskMapper.map(parentTaskEntity, commonSettings.getAppName());
        TaskId joinTaskId1 = taskMapper.map(joinTaskEntity1, commonSettings.getAppName());
        TaskId joinTaskId2 = taskMapper.map(joinTaskEntity2, commonSettings.getAppName());

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTaskId1, parentTaskId),
                        toJoinTaskLink(joinTaskId2, parentTaskId)
                )
        );

        //do
        getTaskWorker().execute(parentTaskEntity, registeredTask);

        //verify
        assertThat(taskRepository.find(parentTaskEntity.getId())).isEmpty();
        assertThat(taskLinkRepository.findAllByJoinTaskIdIn(List.of(joinTaskId1.getId(), joinTaskId2.getId())))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactlyInAnyOrder(
                        toJoinTaskLink(joinTaskId1, parentTaskId).toBuilder().completed(true).build(),
                        toJoinTaskLink(joinTaskId2, parentTaskId).toBuilder().completed(true).build()
                );
    }

    @SneakyThrows
    @Test
    void shouldMarkOutputLinksToJoinTasksAsCompletedWhenOnlyForkedChildren() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.scheduleFork(taskDef, m.withNewMessage("fork-child"));
        });
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
    }

    @SneakyThrows
    @Test
    void shouldProvideJoinTaskMessage() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            //verify
            assertThat(m.getInputJoinTaskMessages()).containsExactlyInAnyOrder("Hello", "world!");
        });
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        TaskEntity parentTaskEntity = saveNewTaskEntity().toBuilder()
                .joinMessageBytes(taskSerializer.writeValue(JoinTaskMessageContainer.builder()
                        .rawMessages(List.of(
                                taskSerializer.writeValue("Hello"),
                                taskSerializer.writeValue("world!")
                        ))
                        .build())
                )
                .build();

        //do
        getTaskWorker().execute(parentTaskEntity, registeredTask);
    }

    @SneakyThrows
    @Test
    void shouldHandleReachableJoinMessages() {
        //when
        TaskDef<String> generalTaskDef = TaskDef.privateTaskDef("test", String.class);
        TaskDef<String> joinTaskDef = TaskDef.privateTaskDef("join-task", String.class);
        TaskDef<String> targetJoinTaskDef = TaskDef.privateTaskDef("target-join-task", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        TaskId joinTask2 = createTaskId("join-task");
        TaskId joinTask3 = createTaskId("join-task");
        TaskId joinTask4 = createTaskId("target-join-task");
        TaskId joinTask5 = createTaskId("target-join-task");
        TaskId joinTask6 = createTaskId("join-task");
        TaskId joinTask7 = createTaskId("join-task");
        TaskId joinTask8 = createTaskId("target-join-task");
        TaskId joinTask9 = createTaskId("join-task");
        TaskId joinTask10 = createTaskId("target-join-task");

        Task<String> mockedTask = TaskGenerator.defineTask(generalTaskDef, m -> {
            //verify
            List<JoinTaskMessage<String>> joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(targetJoinTaskDef);
            assertThat(joinMessagesFromBranch).hasSize(4);

            JoinTaskMessage<String> messageForJoinTask4 = assertMessage(joinMessagesFromBranch, joinTask4, "message_for_4");
            JoinTaskMessage<String> messageForJoinTask5 = assertMessage(joinMessagesFromBranch, joinTask5, null);
            JoinTaskMessage<String> messageForJoinTask8 = assertMessage(joinMessagesFromBranch, joinTask8, null);
            JoinTaskMessage<String> messageForJoinTask10 = assertMessage(joinMessagesFromBranch, joinTask10, "message_for_10");


            distributedTaskService.setJoinMessageToBranch(messageForJoinTask4.toBuilder().message("initial message").build());
            distributedTaskService.setJoinMessageToBranch(messageForJoinTask10.toBuilder().message("initial message").build());

            joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(targetJoinTaskDef);
            assertThat(joinMessagesFromBranch).hasSize(4);

            messageForJoinTask4 = assertMessage(joinMessagesFromBranch, joinTask4, "initial message");
            messageForJoinTask5 = assertMessage(joinMessagesFromBranch, joinTask5, null);
            messageForJoinTask8 = assertMessage(joinMessagesFromBranch, joinTask8, null);
            messageForJoinTask10 = assertMessage(joinMessagesFromBranch, joinTask10, "initial message");

            distributedTaskService.setJoinMessageToBranch(messageForJoinTask4.toBuilder().message("override message").build());
            distributedTaskService.setJoinMessageToBranch(messageForJoinTask5.toBuilder().message("initial message").build());
            distributedTaskService.setJoinMessageToBranch(messageForJoinTask8.toBuilder().message("initial message").build());
            distributedTaskService.setJoinMessageToBranch(messageForJoinTask10.toBuilder().message("override message").build());
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("general-task"))).thenReturn(Optional.of(registeredTask));

        RegisteredTask<String> registeredJoinTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(joinTaskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("join-task"))).thenReturn(Optional.of(registeredJoinTask));

        RegisteredTask<String> registeredTargetJoinTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(targetJoinTaskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("target-join-task"))).thenReturn(Optional.of(registeredTargetJoinTask));

        TaskEntity generalTask1 = saveNewTaskEntity();
        TaskId generalTaskId1 = taskMapper.map(generalTask1, commonSettings.getAppName());

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask2, generalTaskId1),
                        toJoinTaskLink(joinTask4, joinTask2),
                        toJoinTaskLink(joinTask6, joinTask4),

                        toJoinTaskLink(joinTask3, generalTaskId1),
                        toJoinTaskLink(joinTask5, joinTask3),

                        toJoinTaskLink(joinTask7, joinTask5),
                        toJoinTaskLink(joinTask8, joinTask5),

                        toJoinTaskLink(joinTask9, joinTask7),
                        toJoinTaskLink(joinTask10, joinTask7)
                )
        );

        taskMessageRepository.saveAll(
                List.of(
                        toMessage(generalTaskId1, joinTask4, "message_for_4"),
                        toMessage(generalTaskId1, joinTask10, "message_for_10")
                )
        );

        //do
        getTaskWorker().execute(generalTask1, registeredTask);

        //verify
        assertMessage(generalTaskId1, joinTask4, "override message");
        assertMessage(generalTaskId1, joinTask5, "initial message");
        assertMessage(generalTaskId1, joinTask8, "initial message");
        assertMessage(generalTaskId1, joinTask10, "override message");
    }

    @SneakyThrows
    @Test
    void shouldNotSaveJoinMessagesWhenException() {
        //when
        TaskDef<String> generalTaskDef = TaskDef.privateTaskDef("test", String.class);
        TaskDef<String> joinTaskDef = TaskDef.privateTaskDef("join-task", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        TaskId joinTask2 = createTaskId("join-task");

        Task<String> mockedTask = TaskGenerator.defineTask(generalTaskDef, m -> {
            //verify
            List<JoinTaskMessage<String>> joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(joinTaskDef);
            assertThat(joinMessagesFromBranch).hasSize(1);

            JoinTaskMessage<String> messageForJoinTask4 = assertMessage(joinMessagesFromBranch, joinTask2, null);
            distributedTaskService.setJoinMessageToBranch(messageForJoinTask4.toBuilder().message("initial message").build());

            joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(joinTaskDef);
            assertThat(joinMessagesFromBranch).hasSize(1);

            assertMessage(joinMessagesFromBranch, joinTask2, "initial message");
            throw new RuntimeException();
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("general-task"))).thenReturn(Optional.of(registeredTask));

        RegisteredTask<String> registeredJoinTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(joinTaskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("join-task"))).thenReturn(Optional.of(registeredJoinTask));

        TaskEntity generalTask1 = saveNewTaskEntity();
        TaskId generalTaskId1 = taskMapper.map(generalTask1, commonSettings.getAppName());

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask2, generalTaskId1)
                )
        );

        //do
        getTaskWorker().execute(generalTask1, registeredTask);

        //verify
        assertThat(taskMessageRepository.findByTaskToJoinIdAndJoinTaskId(generalTaskId1.getId(), joinTask2.getId()))
                .isEmpty();
    }

    @SneakyThrows
    @Test
    void shouldNotHandleNotReachableJoinMessages() {
        //when
        TaskDef<String> generalTaskDef = TaskDef.privateTaskDef("test", String.class);
        TaskDef<String> joinTaskDef = TaskDef.privateTaskDef("join-task", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        TaskId joinTask2 = createTaskId("join-task");

        Task<String> mockedTask = TaskGenerator.defineTask(generalTaskDef, m -> {
            //verify
            assertThatThrownBy(() -> distributedTaskService.getJoinMessagesFromBranch(joinTaskDef))
                    .isInstanceOf(IllegalArgumentException.class);
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("general-task"))).thenReturn(Optional.of(registeredTask));

        RegisteredTask<String> registeredJoinTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(joinTaskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("join-task"))).thenReturn(Optional.of(registeredJoinTask));

        TaskEntity generalTask1 = saveNewTaskEntity();
        TaskId generalTaskId1 = taskMapper.map(generalTask1, commonSettings.getAppName());

        //do
        getTaskWorker().execute(generalTask1, registeredTask);

        //verify
        assertThat(taskMessageRepository.findByTaskToJoinIdAndJoinTaskId(generalTaskId1.getId(), joinTask2.getId()))
                .isEmpty();
    }

    private void assertMessage(TaskId from, TaskId to, @Nullable String message) {
        assertThat(taskMessageRepository.findByTaskToJoinIdAndJoinTaskId(from.getId(), to.getId()))
                .isPresent()
                .get()
                .satisfies(entity -> assertThat(taskSerializer.readValue(entity.getMessage(), String.class))
                        .isEqualTo(message)
                );
    }

    private JoinTaskMessage<String> assertMessage(List<JoinTaskMessage<String>> joinMessagesFromBranch,
                                                  TaskId joinTask,
                                                  @Nullable String message) {
        List<JoinTaskMessage<String>> joinTaskMessages = joinMessagesFromBranch.stream()
                .filter(realMessage -> realMessage.getTaskId().equals(joinTask))
                .collect(Collectors.toList());
        assertThat(joinTaskMessages).singleElement()
                .matches(origMessage -> Objects.equals(origMessage.getMessage(), message), "message");
        return joinTaskMessages.get(0);
    }
}

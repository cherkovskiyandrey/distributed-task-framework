package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.model.JoinTaskExecution;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.JoinTaskMessageContainer;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskLinkEntity;
import com.distributed_task_framework.persistence.entity.TaskMessageEntity;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.WorkerManager;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
@FieldDefaults(level = AccessLevel.PRIVATE)
class TaskLinkManagerImplIntegrationTest extends BaseSpringIntegrationTest {
    @MockBean
    TaskRegistryService taskRegistryService;
    @MockBean
    WorkerManager workerManager;
    @Autowired
    TaskLinkManager taskLinkManager;

    @Test
    void shouldCreateLinks() {
        //when
        TaskId joinTask = createTaskId("join-task");
        List<TaskId> taskToJoin = List.of(createTaskId("general-task"), createTaskId("general-task"));

        //do
        taskLinkManager.createLinks(joinTask, taskToJoin);

        //verify
        Assertions.assertThat(taskLinkRepository.findAllByJoinTaskName("join-task"))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactlyInAnyOrder(
                        toJoinTaskLink(joinTask, taskToJoin.get(0)),
                        toJoinTaskLink(joinTask, taskToJoin.get(1))
                );
    }

    @Test
    void shouldInheritLinks() {
        //when
        TaskId joinTask1 = createTaskId("join-task1");
        TaskId joinTask2 = createTaskId("join-task2");
        TaskId parentTaskToJoin = createTaskId("parent-task");
        TaskId childTaskToJoin1 = createTaskId("child-task");
        TaskId childTaskToJoin2 = createTaskId("child-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask1, parentTaskToJoin),
                        toJoinTaskLink(joinTask2, parentTaskToJoin)
                )
        );

        //do
        taskLinkManager.inheritLinks(parentTaskToJoin.getId(), Set.of(childTaskToJoin1.getId(), childTaskToJoin2.getId()));

        //verify
        Assertions.assertThat(taskLinkRepository.findAllByJoinTaskName("join-task1"))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactlyInAnyOrder(
                        toJoinTaskLink(joinTask1, parentTaskToJoin),
                        toJoinTaskLink(joinTask1, childTaskToJoin1),
                        toJoinTaskLink(joinTask1, childTaskToJoin2)
                );

        Assertions.assertThat(taskLinkRepository.findAllByJoinTaskName("join-task2"))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactlyInAnyOrder(
                        toJoinTaskLink(joinTask2, parentTaskToJoin),
                        toJoinTaskLink(joinTask2, childTaskToJoin1),
                        toJoinTaskLink(joinTask2, childTaskToJoin2)
                );
    }

    @Test
    void shouldRemoveLinks() {
        //when
        TaskId joinTask1 = createTaskId("join-task1");
        TaskId joinTask2 = createTaskId("join-task2");
        TaskId parentTaskToJoin = createTaskId("parent-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask1, parentTaskToJoin),
                        toJoinTaskLink(joinTask2, parentTaskToJoin)
                )
        );

        //do
        taskLinkManager.removeLinks(parentTaskToJoin.getId());

        //verify
        Assertions.assertThat(taskLinkRepository.findAll()).isEmpty();
    }

    @Test
    void shouldCheckHasLinks() {
        //when
        TaskId joinTask = createTaskId("join-task");
        TaskId generalTask = createTaskId("general-task");

        taskLinkRepository.saveAll(List.of(toJoinTaskLink(joinTask, generalTask)));

        //do & verify
        assertThat(taskLinkManager.hasLinks(generalTask.getId())).isTrue();
    }

    @Test
    void shouldMarkLinksAsCompleted() {
        //when
        TaskId joinTask1 = createTaskId("join-task1");
        TaskId joinTask2 = createTaskId("join-task2");
        TaskId generalTask = createTaskId("general-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask1, generalTask),
                        toJoinTaskLink(joinTask2, generalTask)
                )
        );

        //do
        taskLinkManager.markLinksAsCompleted(generalTask.getId());

        //verify
        Assertions.assertThat(taskLinkRepository.findAllByTaskToJoinId(generalTask.getId()))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .map(TaskLinkEntity::isCompleted)
                .containsExactly(true, true);
    }

    @Test
    void shouldGetReadyToPlanJoinTasks() {
        //when
        TaskId generalTask1 = createTaskId("general-task");
        TaskId generalTask2 = createTaskId("general-task");
        TaskId generalTask3 = createTaskId("general-task");
        TaskId generalTask4 = createTaskId("general-task");
        TaskId generalTask5 = createTaskId("general-task");

        TaskId joinTask6 = createTaskId("join-task");
        TaskId joinTask7 = createTaskId("join-task");
        TaskId joinTask8 = createTaskId("join-task");

        TaskId joinTask9 = createTaskId("join-task");
        TaskId joinTask10 = createTaskId("join-task");

        TaskId joinTask11 = createTaskId("join-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask6, generalTask1),
                        toJoinTaskLink(joinTask6, generalTask2),

                        toJoinTaskLink(joinTask7, generalTask3),
                        toJoinTaskLink(joinTask7, generalTask4),

                        toJoinTaskLink(joinTask8, generalTask4),
                        toJoinTaskLink(joinTask8, generalTask5),

                        toJoinTaskLink(joinTask9, joinTask6),
                        toJoinTaskLink(joinTask9, generalTask3),

                        toJoinTaskLink(joinTask10, joinTask8),

                        toJoinTaskLink(joinTask11, joinTask9),
                        toJoinTaskLink(joinTask11, joinTask7)
                )
        );

        markAsReady(generalTask1);
        markAsReady(generalTask2);
        markAsReady(generalTask3);
        markAsReady(generalTask4);
        markAsReady(generalTask5);

        //do
        List<UUID> readyToPlanJoinTasks = taskLinkManager.getReadyToPlanJoinTasks(100);

        //verify
        assertThat(readyToPlanJoinTasks).containsExactlyInAnyOrder(
                joinTask6.getId(),
                joinTask7.getId(),
                joinTask8.getId()
        );
    }

    @SneakyThrows
    @Test
    void shouldGetJoinMessages() {
        //when
        TaskId generalTask1 = createTaskId("general-task");
        TaskId joinTask2 = createTaskId("join-task");
        TaskId joinTask3 = createTaskId("join-task");
        TaskId joinTask4 = createTaskId("target-join-task");
        TaskId joinTask5 = createTaskId("target-join-task");
        TaskId joinTask6 = createTaskId("join-task");
        TaskId joinTask7 = createTaskId("join-task");
        TaskId joinTask8 = createTaskId("target-join-task");
        TaskId joinTask9 = createTaskId("join-task");
        TaskId joinTask10 = createTaskId("target-join-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask2, generalTask1),
                        toJoinTaskLink(joinTask4, joinTask2),
                        toJoinTaskLink(joinTask6, joinTask4),

                        toJoinTaskLink(joinTask3, generalTask1),
                        toJoinTaskLink(joinTask5, joinTask3),

                        toJoinTaskLink(joinTask7, joinTask5),
                        toJoinTaskLink(joinTask8, joinTask5),

                        toJoinTaskLink(joinTask9, joinTask7),
                        toJoinTaskLink(joinTask10, joinTask7)
                )
        );

        taskMessageRepository.saveAll(
                List.of(
                        toMessage(generalTask1, joinTask4, "message_for_4"),
                        toMessage(generalTask1, joinTask10, "message_for_10")
                )
        );

        //do
        Collection<JoinTaskMessage<String>> joinMessages = taskLinkManager.getJoinMessages(
                generalTask1,
                TaskDef.publicTaskDef("test-app", "target-join-task", String.class)
        );

        //verify
        assertThat(joinMessages)
                .containsExactlyInAnyOrder(
                        JoinTaskMessage.<String>builder()
                                .taskId(joinTask4)
                                .message("message_for_4")
                                .build(),
                        JoinTaskMessage.<String>builder()
                                .taskId(joinTask10)
                                .message("message_for_10")
                                .build(),
                        JoinTaskMessage.<String>builder()
                                .taskId(joinTask5)
                                .build(),
                        JoinTaskMessage.<String>builder()
                                .taskId(joinTask8)
                                .build()
                );
    }

    @SneakyThrows
    @Test
    void shouldSetJoinMessagesWhenFirstTime() {
        //when
        TaskId generalTask1 = createTaskId("general-task");
        TaskId joinTask2 = createTaskId("join-task");
        TaskId joinTask3 = createTaskId("join-task");
        TaskId joinTask4 = createTaskId("target-join-task");
        TaskId joinTask5 = createTaskId("target-join-task");
        TaskId joinTask6 = createTaskId("join-task");
        TaskId joinTask7 = createTaskId("join-task");
        TaskId joinTask8 = createTaskId("target-join-task");
        TaskId joinTask9 = createTaskId("join-task");
        TaskId joinTask10 = createTaskId("target-join-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask2, generalTask1),
                        toJoinTaskLink(joinTask4, joinTask2),
                        toJoinTaskLink(joinTask6, joinTask4),

                        toJoinTaskLink(joinTask3, generalTask1),
                        toJoinTaskLink(joinTask5, joinTask3),

                        toJoinTaskLink(joinTask7, joinTask5),
                        toJoinTaskLink(joinTask8, joinTask5),

                        toJoinTaskLink(joinTask9, joinTask7),
                        toJoinTaskLink(joinTask10, joinTask7)
                )
        );

        taskMessageRepository.saveAll(
                List.of(
                        toMessage(generalTask1, joinTask4, "message_for_4"),
                        toMessage(generalTask1, joinTask10, "message_for_10")
                )
        );

        //do
        taskLinkManager.setJoinMessages(
                generalTask1,
                JoinTaskMessage.builder()
                        .taskId(joinTask8)
                        .message("Hello world!")
                        .build()
        );

        //verify
        Assertions.assertThat(taskMessageRepository.findByTaskToJoinIdAndJoinTaskId(generalTask1.getId(), joinTask8.getId()))
                .isPresent()
                .get()
                .usingRecursiveComparison()
                .ignoringFields("id")
                .isEqualTo(
                        TaskMessageEntity.builder()
                                .taskToJoinId(generalTask1.getId())
                                .joinTaskId(joinTask8.getId())
                                .message(taskSerializer.writeValue("Hello world!"))
                                .build()
                );
    }

    @SneakyThrows
    @Test
    void shouldSetJoinMessageWhenOverride() {
        //when
        TaskId generalTask1 = createTaskId("general-task");
        TaskId joinTask2 = createTaskId("join-task");
        TaskId joinTask3 = createTaskId("join-task");
        TaskId joinTask4 = createTaskId("target-join-task");
        TaskId joinTask5 = createTaskId("target-join-task");
        TaskId joinTask6 = createTaskId("join-task");
        TaskId joinTask7 = createTaskId("join-task");
        TaskId joinTask8 = createTaskId("target-join-task");
        TaskId joinTask9 = createTaskId("join-task");
        TaskId joinTask10 = createTaskId("target-join-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask2, generalTask1),
                        toJoinTaskLink(joinTask4, joinTask2),
                        toJoinTaskLink(joinTask6, joinTask4),

                        toJoinTaskLink(joinTask3, generalTask1),
                        toJoinTaskLink(joinTask5, joinTask3),

                        toJoinTaskLink(joinTask7, joinTask5),
                        toJoinTaskLink(joinTask8, joinTask5),

                        toJoinTaskLink(joinTask9, joinTask7),
                        toJoinTaskLink(joinTask10, joinTask7)
                )
        );

        taskMessageRepository.saveAll(
                List.of(
                        toMessage(generalTask1, joinTask4, "message_for_4"),
                        toMessage(generalTask1, joinTask10, "message_for_10")
                )
        );

        //do
        taskLinkManager.setJoinMessages(
                generalTask1,
                JoinTaskMessage.builder()
                        .taskId(joinTask4)
                        .message("Hello world!")
                        .build()
        );

        //verify
        Assertions.assertThat(taskMessageRepository.findByTaskToJoinIdAndJoinTaskId(generalTask1.getId(), joinTask4.getId()))
                .isPresent()
                .get()
                .usingRecursiveComparison()
                .ignoringFields("id")
                .isEqualTo(
                        TaskMessageEntity.builder()
                                .taskToJoinId(generalTask1.getId())
                                .joinTaskId(joinTask4.getId())
                                .message(taskSerializer.writeValue("Hello world!"))
                                .build()
                );
    }

    @SneakyThrows
    @Test
    void shouldNotSetJoinMessageWhenNotInBranch() {
        //when
        TaskId generalTask1 = createTaskId("general-task");
        TaskId joinTask2 = createTaskId("join-task");
        TaskId joinTask3 = createTaskId("join-task");
        TaskId joinTask4 = createTaskId("target-join-task");
        TaskId joinTask5 = createTaskId("target-join-task");
        TaskId joinTask6 = createTaskId("join-task");
        TaskId joinTask7 = createTaskId("join-task");
        TaskId joinTask8 = createTaskId("target-join-task");
        TaskId joinTask9 = createTaskId("join-task");
        TaskId joinTask10 = createTaskId("target-join-task");

        TaskId generalTask11 = createTaskId("general-task");
        TaskId joinTask12 = createTaskId("foreign-join-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask2, generalTask1),
                        toJoinTaskLink(joinTask4, joinTask2),
                        toJoinTaskLink(joinTask6, joinTask4),

                        toJoinTaskLink(joinTask3, generalTask1),
                        toJoinTaskLink(joinTask5, joinTask3),

                        toJoinTaskLink(joinTask7, joinTask5),
                        toJoinTaskLink(joinTask8, joinTask5),

                        toJoinTaskLink(joinTask9, joinTask7),
                        toJoinTaskLink(joinTask10, joinTask7),

                        toJoinTaskLink(joinTask12, generalTask11)
                )
        );

        taskMessageRepository.saveAll(
                List.of(
                        toMessage(generalTask1, joinTask4, "message_for_4"),
                        toMessage(generalTask1, joinTask10, "message_for_10"),
                        toMessage(generalTask11, joinTask12, "message_for_12")
                )
        );

        //do
        assertThatThrownBy(() -> {
            taskLinkManager.setJoinMessages(
                    generalTask1,
                    JoinTaskMessage.builder()
                            .taskId(joinTask12)
                            .message("Hello world!")
                            .build()
            );
        }).isInstanceOf(IllegalArgumentException.class);

        //verify
        Assertions.assertThat(taskMessageRepository.findByTaskToJoinIdAndJoinTaskId(generalTask11.getId(), joinTask12.getId()))
                .isPresent()
                .get()
                .usingRecursiveComparison()
                .ignoringFields("id")
                .isEqualTo(
                        TaskMessageEntity.builder()
                                .taskToJoinId(generalTask11.getId())
                                .joinTaskId(joinTask12.getId())
                                .message(taskSerializer.writeValue("message_for_12"))
                                .build()
                );
    }

    @Test
    void shouldDetectLeaves() {
        //when
        TaskId generalTask1 = createTaskId("general-task");
        TaskId joinTask2 = createTaskId("join-task");
        TaskId joinTask3 = createTaskId("join-task");
        TaskId joinTask4 = createTaskId("target-join-task");
        TaskId joinTask5 = createTaskId("target-join-task");
        TaskId joinTask6 = createTaskId("join-task");
        TaskId joinTask7 = createTaskId("join-task");
        TaskId joinTask8 = createTaskId("target-join-task");
        TaskId joinTask9 = createTaskId("join-task");
        TaskId joinTask10 = createTaskId("target-join-task");

        TaskId generalTask11 = createTaskId("general-task");
        TaskId joinTask12 = createTaskId("foreign-join-task");

        TaskId generalTask13 = createTaskId("general-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask2, generalTask1),
                        toJoinTaskLink(joinTask4, joinTask2),
                        toJoinTaskLink(joinTask6, joinTask4),

                        toJoinTaskLink(joinTask3, generalTask1),
                        toJoinTaskLink(joinTask5, joinTask3),

                        toJoinTaskLink(joinTask7, joinTask5),
                        toJoinTaskLink(joinTask8, joinTask5),

                        toJoinTaskLink(joinTask9, joinTask7),
                        toJoinTaskLink(joinTask10, joinTask7),

                        toJoinTaskLink(joinTask12, generalTask11)
                )
        );

        //do
        Set<UUID> uuids = taskLinkManager.detectLeaves(Set.of(
                generalTask1.getId(),
                joinTask2.getId(),
                joinTask3.getId(),
                joinTask4.getId(),
                joinTask5.getId(),
                joinTask6.getId(),
                joinTask7.getId(),
                joinTask8.getId(),
                joinTask9.getId(),
                joinTask10.getId(),
                generalTask11.getId(),
                joinTask12.getId(),
                generalTask13.getId()
        ));

        //verify
        assertThat(uuids).containsExactlyInAnyOrder(
                joinTask6.getId(),
                joinTask9.getId(),
                joinTask10.getId(),
                joinTask8.getId(),
                joinTask12.getId(),
                generalTask13.getId()
        );
    }

    @Test
    void shouldPrepareJoinTaskToPlan() {
        //when
        //prev tasks
        TaskId generalPrevTask1 = createTaskId("general-task");
        TaskId generalPrevTask2 = createTaskId("general-task");
        TaskId generalPrevTask3 = createTaskId("general-task");

        //current tasks
        TaskId generalTask1 = createTaskId("general-task");
        TaskId generalTask2 = createTaskId("general-task");
        TaskId generalTask3 = createTaskId("general-task");
        TaskId joinTask = createTaskId("join-task");

        taskLinkRepository.saveAll(
                List.of(
                        toJoinTaskLink(joinTask, generalTask1),
                        toJoinTaskLink(joinTask, generalTask2),
                        toJoinTaskLink(joinTask, generalTask3)
                )
        );

        taskMessageRepository.saveAll(
                List.of(
                        toMessage(generalPrevTask1, joinTask, "prev_message_1"),
                        toMessage(generalPrevTask2, joinTask, "prev_message_2"),
                        toMessage(generalPrevTask3, joinTask, "prev_message_3"),

                        toMessage(generalTask1, joinTask, "message_1"),
                        toMessage(generalTask2, joinTask, "message_2"),
                        toMessage(generalTask3, joinTask, "message_3")
                )
        );

        //do
        List<JoinTaskExecution> joinTaskExecutions = taskLinkManager.prepareJoinTaskToPlan(List.of(joinTask.getId()));

        //verify
        Assertions.assertThat(taskLinkRepository.findAll()).isEmpty();
        Assertions.assertThat(taskMessageRepository.findAll()).isEmpty();

        assertThat(joinTaskExecutions).singleElement()
                .matches(joinTaskExecution -> joinTask.getId().equals(joinTaskExecution.getTaskId()), "taskId")
                .satisfies(joinTaskExecution -> Assertions.assertThat(taskSerializer.readValue(
                                joinTaskExecution.getJoinedMessage(),
                                JoinTaskMessageContainer.class
                        ))
                                .satisfies(joinTaskMessageContainer -> Assertions.assertThat(joinTaskMessageContainer.getRawMessages())
                                        .map(bytes -> taskSerializer.readValue(bytes, String.class))
                                        .containsExactlyInAnyOrder(
                                                "prev_message_1",
                                                "prev_message_2",
                                                "prev_message_3",
                                                "message_1",
                                                "message_2",
                                                "message_3"
                                        )
                                )
                )
        ;
    }
}

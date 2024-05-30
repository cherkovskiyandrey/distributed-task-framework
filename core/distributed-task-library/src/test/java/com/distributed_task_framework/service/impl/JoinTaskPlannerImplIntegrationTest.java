package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.JoinTaskMessageContainer;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.PlannerRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.WorkerManager;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class JoinTaskPlannerImplIntegrationTest extends BaseSpringIntegrationTest {
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    PlannerRepository plannerRepository;
    @Autowired
    TaskRegistryService taskRegistryService;
    @Autowired
    PlatformTransactionManager transactionManager;
    @Autowired
    TaskLinkManager taskLinkManager;
    @Autowired
    TaskRepository taskRepository;
    @Autowired
    MetricHelper metricHelper;
    @Autowired
    JoinTaskStatHelper statHelper;
    JoinTaskPlannerImpl plannerService;
    ExecutorService executorService;

    @BeforeEach
    public void init() {
        super.init();
        executorService = Executors.newSingleThreadExecutor();
        plannerService = Mockito.spy(new JoinTaskPlannerImpl(
                commonSettings,
                plannerRepository,
                transactionManager,
                clusterProvider,
                taskLinkManager,
                taskRepository,
                metricHelper,
                statHelper,
                clock
        ));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @SneakyThrows
    @AfterEach
    void destroy() {
        plannerService.shutdown();
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    void shouldPlanReadyJoinTasks() {
        //when
        TaskDef<String> generalTaskDef = TaskDef.privateTaskDef("general-task", String.class);
        TaskDef<String> joinTaskDef = TaskDef.privateTaskDef("join-task", String.class);
        waitForNodeIsRegistered(generalTaskDef, joinTaskDef);

        TaskId generalTask1 = createTaskId("general-task");
        TaskId generalTask2 = createTaskId("general-task");
        TaskId generalTask3 = createTaskId("general-task");
        TaskId generalTask4 = createTaskId("general-task");
        TaskId generalTask5 = createTaskId("general-task");

        TaskId joinTask6 = createAndRegisterJoinTask("join-task");
        TaskId joinTask7 = createAndRegisterJoinTask("join-task");
        TaskId joinTask8 = createAndRegisterJoinTask("join-task");

        TaskId joinTask9 = createAndRegisterJoinTask("join-task");
        TaskId joinTask10 = createAndRegisterJoinTask("join-task");

        TaskId joinTask11 = createAndRegisterJoinTask("join-task");

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

        taskMessageRepository.saveAll(
                List.of(
                        toMessage(generalTask1, joinTask6, "message_to_6_from_1"),
                        toMessage(generalTask2, joinTask6, "message_to_6_from_2"),

                        toMessage(generalTask3, joinTask7, "message_to_7_from_3"),
                        toMessage(generalTask4, joinTask7, "message_to_7_from_4")
                )
        );

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        waitFor(() -> taskRepository.findAll(List.of(
                joinTask6.getId(),
                joinTask7.getId(),
                joinTask8.getId()
        )).stream().noneMatch(TaskEntity::isNotToPlan));

        assertMessages(joinTask6, List.of("message_to_6_from_1", "message_to_6_from_2"));
        assertMessages(joinTask7, List.of("message_to_7_from_3", "message_to_7_from_4"));
        assertNoneMessages(joinTask8);

        Assertions.assertThat(taskLinkRepository.findAllByJoinTaskIdIn(List.of(
                joinTask6.getId(),
                joinTask7.getId(),
                joinTask8.getId()
        ))).isEmpty();

        Assertions.assertThat(taskMessageRepository.findAllByJoinTaskIdIn(List.of(
                joinTask6.getId(),
                joinTask7.getId(),
                joinTask8.getId()
        ))).isEmpty();

        Assertions.assertThat(taskRepository.findAll(List.of(
                joinTask9.getId(),
                joinTask10.getId(),
                joinTask11.getId()))
        ).map(TaskEntity::isNotToPlan)
                .allMatch(Boolean.TRUE::equals);
    }

    private void assertNoneMessages(TaskId joinTask) {
        Assertions.assertThat(taskRepository.find(joinTask.getId()))
                .isPresent()
                .get()
                .satisfies(taskEntity -> Assertions.assertThat(taskSerializer.readValue(taskEntity.getJoinMessageBytes(), JoinTaskMessageContainer.class))
                        .satisfies(joinTaskMessage -> Assertions.assertThat(joinTaskMessage.getRawMessages()).isEmpty())
                );
    }

    private void assertMessages(TaskId joinTask, List<String> expectedMessages) {
        Assertions.assertThat(taskRepository.find(joinTask.getId()))
                .isPresent()
                .get()
                .satisfies(taskEntity -> Assertions.assertThat(taskSerializer.readValue(taskEntity.getJoinMessageBytes(), JoinTaskMessageContainer.class))
                        .satisfies(joinTaskMessage -> Assertions.assertThat(joinTaskMessage.getRawMessages())
                                .map(bytes -> taskSerializer.readValue(bytes, String.class))
                                .containsExactlyInAnyOrderElementsOf(expectedMessages)
                        )
                );
    }
}

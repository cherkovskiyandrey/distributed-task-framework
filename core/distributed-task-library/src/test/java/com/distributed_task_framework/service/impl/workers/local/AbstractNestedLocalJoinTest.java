package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.JoinTaskMessageContainer;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.utils.TaskGenerator;
import com.distributed_task_framework.task.TestTaskModelSpec;
import com.distributed_task_framework.utils.JdbcTools;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractNestedLocalJoinTest extends BaseLocalWorkerIntegrationTest {

    TaskDef<String> taskDef;

    @BeforeEach
    public void init() {
        super.init();
        var childTaskTestModel = extendedTaskGenerator.generateDefault(String.class);
        this.taskDef = childTaskTestModel.getTaskDef();
    }

    @ParameterizedTest
    @EnumSource(ActionMode.class)
    @SneakyThrows
    void shouldMoveOutputLinksToJoinTasksOnlyToLeavesTasksWhenChildrenNotForked(ActionMode actionMode) {
        //when
        AtomicReference<TaskId> joinTaskId10Ref = new AtomicReference<>();
        AtomicReference<TaskId> joinTaskId11Ref = new AtomicReference<>();

        var parentTestTaskModel = buildActionAndGenerateTask(
            m -> {
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
            },
            String.class,
            actionMode
        );

        TaskId parentTaskId = parentTestTaskModel.getTaskId();

        var joinTaskTestModel1 = extendedTaskGenerator.generateDefaultAndSave(String.class);
        var joinTaskTestModel2 = extendedTaskGenerator.generateDefaultAndSave(String.class);

        TaskId joinTaskId1 = joinTaskTestModel1.getTaskId();
        TaskId joinTaskId2 = joinTaskTestModel2.getTaskId();

        taskLinkRepository.saveAll(
            List.of(
                toJoinTaskLink(joinTaskId1, parentTaskId),
                toJoinTaskLink(joinTaskId2, parentTaskId)
            )
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        assertThat(joinTaskId10Ref.get()).isNotNull();
        assertThat(joinTaskId11Ref.get()).isNotNull();
        verifyLocalTaskIsFinished(parentTestTaskModel.getTaskEntity());
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

    @Test
    @SneakyThrows
    void shouldWinOnFailureWhenMoveOutputLinks() {
        //when
        var leafJoinTestTaskModel = extendedTaskGenerator.generateJoinByNameAndSave(String.class, "leaf_join");

        var intermediateFromExecuteTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var intermediateJoinFromExecuteTestTaskModel = extendedTaskGenerator.generateDefault(String.class);

        var intermediateFromFailedTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var intermediateJoinFromFailedTestTaskModel = extendedTaskGenerator.generateDefault(String.class);

        var intermediateJoinFromExecuteTaskId = new AtomicReference<TaskId>(null);
        var intermediateJoinFromFailedTaskId = new AtomicReference<TaskId>(null);

        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                    var taskId = distributedTaskService.schedule(intermediateFromExecuteTestTaskModel.getTaskDef(), ctx.withEmptyMessage());
                    var joinTaskId = distributedTaskService.scheduleJoin(intermediateJoinFromExecuteTestTaskModel.getTaskDef(), ctx.withEmptyMessage(), List.of(taskId));
                    intermediateJoinFromExecuteTaskId.set(joinTaskId);
                    throw new RuntimeException();
                }
            )
            .failureAction(ctx -> {
                var taskId = distributedTaskService.schedule(intermediateFromFailedTestTaskModel.getTaskDef(), ctx.withEmptyMessage());
                var joinTaskId = distributedTaskService.scheduleJoin(intermediateJoinFromFailedTestTaskModel.getTaskDef(), ctx.withEmptyMessage(), List.of(taskId));
                intermediateJoinFromFailedTaskId.set(joinTaskId);
                return true;
            })
            .build()
        );

        taskLinkRepository.saveAll(List.of(
                toJoinTaskLink(leafJoinTestTaskModel.getTaskId(), parentTestTaskModel.getTaskId())
            )
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        assertThat(intermediateJoinFromExecuteTaskId).isNotNull();
        assertThat(intermediateJoinFromFailedTaskId).isNotNull();
        verifyLocalTaskIsFinished(parentTestTaskModel.getTaskEntity());
        verifyIsEmptyByTaskDef(intermediateFromExecuteTestTaskModel.getTaskDef());
        verifyIsEmptyByTaskDef(intermediateJoinFromExecuteTestTaskModel.getTaskDef());
        assertThat(taskLinkRepository.filterIntermediateTasks(JdbcTools.UUIDsToStringArray(List.of(parentTestTaskModel.getTaskId().getId()))))
            .isEmpty();
        assertThat(taskLinkRepository.findAllByJoinTaskIdIn(List.of(leafJoinTestTaskModel.getTaskId().getId())))
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
            .containsExactlyInAnyOrder(
                toJoinTaskLink(leafJoinTestTaskModel.getTaskId(), intermediateJoinFromFailedTaskId.get())
            );
    }

    @Test
    @SneakyThrows
    void shouldNotMoveOutputLinksToJoinTasksWhenNotLastOnFailure() {
        //when
        var leafJoinTestTaskModel = extendedTaskGenerator.generateJoinByNameAndSave(String.class, "leaf_join");

        var intermediateFromExecuteTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var intermediateJoinFromExecuteTestTaskModel = extendedTaskGenerator.generateDefault(String.class);

        var intermediateFromFailedTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var intermediateJoinFromFailedTestTaskModel = extendedTaskGenerator.generateDefault(String.class);

        var intermediateJoinFromExecuteTaskId = new AtomicReference<TaskId>(null);
        var intermediateJoinFromFailedTaskId = new AtomicReference<TaskId>(null);

        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                    var taskId = distributedTaskService.schedule(intermediateFromExecuteTestTaskModel.getTaskDef(), ctx.withEmptyMessage());
                    var joinTaskId = distributedTaskService.scheduleJoin(intermediateJoinFromExecuteTestTaskModel.getTaskDef(), ctx.withEmptyMessage(), List.of(taskId));
                    intermediateJoinFromExecuteTaskId.set(joinTaskId);
                    throw new RuntimeException();
                }
            )
            .failureAction(ctx -> {
                var taskId = distributedTaskService.schedule(intermediateFromFailedTestTaskModel.getTaskDef(), ctx.withEmptyMessage());
                var joinTaskId = distributedTaskService.scheduleJoin(intermediateJoinFromFailedTestTaskModel.getTaskDef(), ctx.withEmptyMessage(), List.of(taskId));
                intermediateJoinFromFailedTaskId.set(joinTaskId);
                return false;
            })
            .build()
        );

        taskLinkRepository.saveAll(List.of(
                toJoinTaskLink(leafJoinTestTaskModel.getTaskId(), parentTestTaskModel.getTaskId())
            )
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        assertThat(intermediateJoinFromExecuteTaskId).isNotNull();
        assertThat(intermediateJoinFromFailedTaskId).isNotNull();

        assertThat(taskRepository.find(parentTestTaskModel.getTaskId().getId()))
            .isPresent()
            .get()
            .matches(taskEntity -> taskEntity.getFailures() == 1);

        verifyIsEmptyByTaskDef(intermediateFromExecuteTestTaskModel.getTaskDef());
        verifyIsEmptyByTaskDef(intermediateJoinFromExecuteTestTaskModel.getTaskDef());
        verifyIsEmptyByTaskDef(intermediateFromFailedTestTaskModel.getTaskDef());
        verifyIsEmptyByTaskDef(intermediateJoinFromFailedTestTaskModel.getTaskDef());
    }

    @ParameterizedTest
    @EnumSource(ActionMode.class)
    @SneakyThrows
    void shouldNotScheduleCronJoinTask(ActionMode actionMode) {
        //when
        var recurrentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class).recurrent().build());
        var recurrentTaskDef = recurrentTestTaskModel.getTaskDef();

        var parentTestTaskModel = buildActionAndGenerateTask(
            m -> {
                TaskId taskId = distributedTaskService.schedule(taskDef, m.withNewMessage("general"));

                //verify
                assertThatThrownBy(() -> distributedTaskService.scheduleJoin(recurrentTaskDef, m.withNewMessage("join"), List.of(taskId)))
                    .isInstanceOf(TaskConfigurationException.class);
            },
            String.class,
            actionMode
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());
    }


    @SneakyThrows
    @ParameterizedTest
    @EnumSource(ActionMode.class)
    void shouldNotScheduleJoinTaskWhenJoinToCronTask(ActionMode actionMode) {
        //when
        var recurrentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class).recurrent().build());
        var recurrentTaskDef = recurrentTestTaskModel.getTaskDef();

        var parentTestTaskModel = buildActionAndGenerateTask(
            m -> {
                TaskId taskId = distributedTaskService.schedule(recurrentTaskDef, m.withNewMessage("general"));

                //verify
                assertThatThrownBy(() -> distributedTaskService.scheduleJoin(taskDef, m.withNewMessage("join"), List.of(taskId)))
                    .isInstanceOf(TaskConfigurationException.class);
            },
            String.class,
            actionMode
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());
    }

    @SneakyThrows
    @ParameterizedTest
    @EnumSource(ActionMode.class)
    void shouldMarkOutputLinksToJoinTasksAsCompletedWhenNotChildren(ActionMode actionMode) {
        //when
        var parentTestTaskModel = buildActionAndGenerateTask(
            m -> {
            },
            String.class,
            actionMode
        );
        var joinTestTaskModel1 = extendedTaskGenerator.generate(
            TestTaskModelSpec.builder(String.class)
                .withSaveInstance()
                .taskEntityCustomizer(TestTaskModelSpec.JOIN_TASK)
                .build()
        );
        var joinTestTaskModel2 = extendedTaskGenerator.generate(
            TestTaskModelSpec.builder(String.class)
                .withSaveInstance()
                .taskEntityCustomizer(TestTaskModelSpec.JOIN_TASK)
                .build()
        );

        var parentTaskId = parentTestTaskModel.getTaskId();
        var joinTaskId1 = joinTestTaskModel1.getTaskId();
        var joinTaskId2 = joinTestTaskModel2.getTaskId();

        taskLinkRepository.saveAll(
            List.of(
                toJoinTaskLink(joinTaskId1, parentTaskId),
                toJoinTaskLink(joinTaskId2, parentTaskId)
            )
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        verifyLocalTaskIsFinished(parentTestTaskModel.getTaskEntity());
        assertThat(taskLinkRepository.findAllByJoinTaskIdIn(List.of(joinTaskId1.getId(), joinTaskId2.getId())))
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
            .containsExactlyInAnyOrder(
                toJoinTaskLink(joinTaskId1, parentTaskId).toBuilder().completed(true).build(),
                toJoinTaskLink(joinTaskId2, parentTaskId).toBuilder().completed(true).build()
            );
    }

    @SneakyThrows
    @ParameterizedTest
    @EnumSource(ActionMode.class)
    void shouldMarkOutputLinksToJoinTasksAsCompletedWhenOnlyForkedChildren(ActionMode actionMode) {
        //when
        var parentTestTaskModel = buildActionAndGenerateTask(
            m -> distributedTaskService.scheduleFork(taskDef, m.withNewMessage("fork-child")),
            String.class,
            actionMode
        );
        var joinTestTaskModel1 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, "join1");
        var joinTestTaskModel2 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, "join2");

        var parentTaskId = parentTestTaskModel.getTaskId();
        var joinTaskId1 = joinTestTaskModel1.getTaskId();
        var joinTaskId2 = joinTestTaskModel2.getTaskId();

        taskLinkRepository.saveAll(
            List.of(
                toJoinTaskLink(joinTaskId1, parentTaskId),
                toJoinTaskLink(joinTaskId2, parentTaskId)
            )
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        verifyLocalTaskIsFinished(parentTestTaskModel.getTaskEntity());
        assertThat(taskLinkRepository.findAllByJoinTaskIdIn(List.of(joinTaskId1.getId(), joinTaskId2.getId())))
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
            .containsExactlyInAnyOrder(
                toJoinTaskLink(joinTaskId1, parentTaskId).toBuilder().completed(true).build(),
                toJoinTaskLink(joinTaskId2, parentTaskId).toBuilder().completed(true).build()
            );
    }

    @SneakyThrows
    @ParameterizedTest
    @EnumSource(ActionMode.class)
    void shouldProvideJoinTaskMessage(ActionMode actionMode) {
        //when
        TaskGenerator.Consumer<ExecutionContext<String>> action = m -> {
            //verify
            assertThat(m.getInputJoinTaskMessages()).containsExactlyInAnyOrder("Hello", "world!");
        };
        var actionBuilder = buildAction(action, actionMode);

        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(actionBuilder.action())
            .failureAction(actionBuilder.failureAction())
            .taskEntityCustomizer(taskEntity -> {
                    try {
                        return taskEntity.toBuilder()
                            .joinMessageBytes(taskSerializer.writeValue(JoinTaskMessageContainer.builder()
                                    .rawMessages(
                                        List.of(
                                            taskSerializer.writeValue("Hello"),
                                            taskSerializer.writeValue("world!")
                                        )
                                    )
                                    .build()
                                )
                            )
                            .build();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            )
            .build()
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());
    }

    @SneakyThrows
    @ParameterizedTest
    @EnumSource(ActionMode.class)
    void shouldHandleReachableJoinMessages(ActionMode actionMode) {
        //when
        String joinTaskName = "join-task";
        String targetJoinTaskName = "target-join-task";

        var joinTask2 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, joinTaskName);
        var joinTask3 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, joinTaskName);
        var joinTask4 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, targetJoinTaskName);
        var joinTask5 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, targetJoinTaskName);
        var joinTask6 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, joinTaskName);
        var joinTask7 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, joinTaskName);
        var joinTask8 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, targetJoinTaskName);
        var joinTask9 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, joinTaskName);
        var joinTask10 = extendedTaskGenerator.generateJoinByNameAndSave(String.class, targetJoinTaskName);

        var targetJoinTaskDef = joinTask4.getTaskDef();

        var parentTestTaskModel = buildActionAndGenerateTask(
            m -> {
                //verify
                List<JoinTaskMessage<String>> joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(targetJoinTaskDef);
                assertThat(joinMessagesFromBranch).hasSize(4);

                JoinTaskMessage<String> messageForJoinTask4 = assertMessage(joinMessagesFromBranch, joinTask4.getTaskId(), "message_for_4");
                JoinTaskMessage<String> messageForJoinTask5 = assertMessage(joinMessagesFromBranch, joinTask5.getTaskId(), null);
                JoinTaskMessage<String> messageForJoinTask8 = assertMessage(joinMessagesFromBranch, joinTask8.getTaskId(), null);
                JoinTaskMessage<String> messageForJoinTask10 = assertMessage(joinMessagesFromBranch, joinTask10.getTaskId(), "message_for_10");


                distributedTaskService.setJoinMessageToBranch(messageForJoinTask4.toBuilder().message("initial message").build());
                distributedTaskService.setJoinMessageToBranch(messageForJoinTask10.toBuilder().message("initial message").build());

                joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(targetJoinTaskDef);
                assertThat(joinMessagesFromBranch).hasSize(4);

                messageForJoinTask4 = assertMessage(joinMessagesFromBranch, joinTask4.getTaskId(), "initial message");
                messageForJoinTask5 = assertMessage(joinMessagesFromBranch, joinTask5.getTaskId(), null);
                messageForJoinTask8 = assertMessage(joinMessagesFromBranch, joinTask8.getTaskId(), null);
                messageForJoinTask10 = assertMessage(joinMessagesFromBranch, joinTask10.getTaskId(), "initial message");

                distributedTaskService.setJoinMessageToBranch(messageForJoinTask4.toBuilder().message("override message").build());
                distributedTaskService.setJoinMessageToBranch(messageForJoinTask5.toBuilder().message("initial message").build());
                distributedTaskService.setJoinMessageToBranch(messageForJoinTask8.toBuilder().message("initial message").build());
                distributedTaskService.setJoinMessageToBranch(messageForJoinTask10.toBuilder().message("override message").build());
            },
            String.class,
            actionMode
        );

        taskLinkRepository.saveAll(
            List.of(
                toJoinTaskLink(joinTask2.getTaskId(), parentTestTaskModel.getTaskId()),
                toJoinTaskLink(joinTask4.getTaskId(), joinTask2.getTaskId()),
                toJoinTaskLink(joinTask6.getTaskId(), joinTask4.getTaskId()),

                toJoinTaskLink(joinTask3.getTaskId(), parentTestTaskModel.getTaskId()),
                toJoinTaskLink(joinTask5.getTaskId(), joinTask3.getTaskId()),

                toJoinTaskLink(joinTask7.getTaskId(), joinTask5.getTaskId()),
                toJoinTaskLink(joinTask8.getTaskId(), joinTask5.getTaskId()),

                toJoinTaskLink(joinTask9.getTaskId(), joinTask7.getTaskId()),
                toJoinTaskLink(joinTask10.getTaskId(), joinTask7.getTaskId())
            )
        );

        taskMessageRepository.saveAll(
            List.of(
                toMessage(parentTestTaskModel.getTaskId(), joinTask4.getTaskId(), "message_for_4"),
                toMessage(parentTestTaskModel.getTaskId(), joinTask10.getTaskId(), "message_for_10")
            )
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        assertMessage(parentTestTaskModel.getTaskId(), joinTask4.getTaskId(), "override message");
        assertMessage(parentTestTaskModel.getTaskId(), joinTask5.getTaskId(), "initial message");
        assertMessage(parentTestTaskModel.getTaskId(), joinTask8.getTaskId(), "initial message");
        assertMessage(parentTestTaskModel.getTaskId(), joinTask10.getTaskId(), "override message");
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    @SneakyThrows
    void shouldWinOnFailureWhenSendJoinMessage() {
        //when
        var messageFromAction = "message from action";
        var messageFromFailureAction = "message from failureAction";

        var joinTestTaskModel = extendedTaskGenerator.generateJoinByNameAndSave(String.class, "join");
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                var joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(joinTestTaskModel.getTaskDef());
                var joinTaskMessage = assertMessage(joinMessagesFromBranch, joinTestTaskModel.getTaskId(), null);
                distributedTaskService.setJoinMessageToBranch(joinTaskMessage.toBuilder().message(messageFromAction).build());
                throw new RuntimeException();
            })
            .failureAction(ctx -> {
                var joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(joinTestTaskModel.getTaskDef());
                var joinTaskMessage = assertMessage(joinMessagesFromBranch, joinTestTaskModel.getTaskId(), null);
                distributedTaskService.setJoinMessageToBranch(joinTaskMessage.toBuilder().message(messageFromFailureAction).build());
                return true;
            })
            .build()
        );

        taskLinkRepository.saveAll(
            List.of(
                toJoinTaskLink(joinTestTaskModel.getTaskId(), parentTestTaskModel.getTaskId())
            )
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        assertMessage(parentTestTaskModel.getTaskId(), joinTestTaskModel.getTaskId(), messageFromFailureAction);
    }

    @SneakyThrows
    @Test
    void shouldNotSaveJoinMessagesWhenException() {
        //when
        var joinTestTaskModel = extendedTaskGenerator.generateJoinByNameAndSave(String.class, "join");

        TaskGenerator.Consumer<ExecutionContext<String>> action = m -> {
            //verify
            List<JoinTaskMessage<String>> joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(joinTestTaskModel.getTaskDef());
            assertThat(joinMessagesFromBranch).hasSize(1);

            JoinTaskMessage<String> messageForJoinTask = assertMessage(joinMessagesFromBranch, joinTestTaskModel.getTaskId(), null);
            distributedTaskService.setJoinMessageToBranch(messageForJoinTask.toBuilder().message("initial message").build());

            joinMessagesFromBranch = distributedTaskService.getJoinMessagesFromBranch(joinTestTaskModel.getTaskDef());
            assertThat(joinMessagesFromBranch).hasSize(1);

            assertMessage(joinMessagesFromBranch, joinTestTaskModel.getTaskId(), "initial message");
            throw new RuntimeException();
        };
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .taskEntityCustomizer(extendedTaskGenerator.withLastAttempt())
            .action(action)
            .failureAction(ctx -> {
                action.accept(ctx);
                return false;
            })
            .build()
        );

        taskLinkRepository.saveAll(
            List.of(
                toJoinTaskLink(joinTestTaskModel.getTaskId(), parentTestTaskModel.getTaskId())
            )
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        assertThat(taskMessageRepository.findByTaskToJoinIdAndJoinTaskId(
                parentTestTaskModel.getTaskId().getId(),
                joinTestTaskModel.getTaskId().getId()
            )
        ).isEmpty();
    }

    @SneakyThrows
    @ParameterizedTest
    @EnumSource(ActionMode.class)
    void shouldNotHandleNotReachableJoinMessages(ActionMode actionMode) {
        //when
        var joinTestTaskModel = extendedTaskGenerator.generateJoinByNameAndSave(String.class, "join");
        var parentTestTaskModel = buildActionAndGenerateTask(
            m -> {
                //verify
                assertThatThrownBy(() -> distributedTaskService.getJoinMessagesFromBranch(joinTestTaskModel.getTaskDef()))
                    .isInstanceOf(IllegalArgumentException.class);
            },
            String.class,
            actionMode
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        assertThat(taskMessageRepository.findByTaskToJoinIdAndJoinTaskId(
                parentTestTaskModel.getTaskId().getId(),
                joinTestTaskModel.getTaskId().getId()
            )
        ).isEmpty();
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

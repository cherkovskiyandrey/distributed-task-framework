package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelOne;
import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelTwoDto;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;

import java.util.List;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
public class ChildTaskLevelOne extends BaseTask<SimpleMessageDto> {
    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<SimpleMessageDto> getDef() {
        return PrivateTaskDefinitions.CHILD_TASK_LEVEL_ONE;
    }

    @Override
    public void execute(ExecutionContext<SimpleMessageDto> executionContext) throws Exception {
        log.info("execute(): start task: {}", executionContext.getInputMessageOrThrow().getMessage());
        JoinTaskMessage<JoinTaskLevelOne> joinMessagesLevelOne = distributedTaskService.getJoinMessagesFromBranch(PrivateTaskDefinitions.JOIN_TASK_LEVEL_ONE).get(0);
        joinMessagesLevelOne = joinMessagesLevelOne.toBuilder()
                .message(JoinTaskLevelOne.builder()
                        .message("hello from ChildTaskLevelOne: " + executionContext.getInputMessageOrThrow().getMessage())
                        .build())
                .build();
        distributedTaskService.setJoinMessageToBranch(joinMessagesLevelOne);

        List<TaskId> tasksToJoin = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            TaskId taskId = distributedTaskService.schedule(
                    PrivateTaskDefinitions.CHILD_TASK_LEVEL_TWO,
                    executionContext.withNewMessage(SimpleMessageDto.builder()
                            .message("hello!!!")
                            .build()
                    )
            );
            tasksToJoin.add(taskId);
        }

        distributedTaskService.scheduleJoin(
                PrivateTaskDefinitions.JOIN_TASK_LEVEL_TWO,
                executionContext.withNewMessage(JoinTaskLevelTwoDto.builder().build()),
                tasksToJoin
        );
        log.info("execute(): stop task: {}", executionContext.getInputMessageOrThrow().getMessage());
    }
}

package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelOne;
import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelTwoDto;
import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.settings.TaskSettings;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
public class ChildTaskLevelTwo extends BaseTask<SimpleMessageDto> {
    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<SimpleMessageDto> getDef() {
        return PrivateTaskDefinitions.CHILD_TASK_LEVEL_TWO;
    }

    @Override
    public void execute(ExecutionContext<SimpleMessageDto> executionContext) throws Exception {
        log.info("execute(): start task: {}", executionContext.getInputMessageOrThrow().getMessage());

        //add data to join task level one
        JoinTaskMessage<JoinTaskLevelOne> joinMessagesLevelOne = distributedTaskService.getJoinMessagesFromBranch(PrivateTaskDefinitions.JOIN_TASK_LEVEL_ONE).get(0);
        joinMessagesLevelOne = joinMessagesLevelOne.toBuilder()
                .message(JoinTaskLevelOne.builder()
                        .message("hello from ChildTaskLevelTwo: " + executionContext.getInputMessageOrThrow().getMessage())
                        .build())
                .build();
        distributedTaskService.setJoinMessageToBranch(joinMessagesLevelOne);

        //add data to join task level two
        JoinTaskMessage<JoinTaskLevelTwoDto> joinTaskLevelTwo = distributedTaskService.getJoinMessagesFromBranch(PrivateTaskDefinitions.JOIN_TASK_LEVEL_TWO).get(0);
        joinTaskLevelTwo = joinTaskLevelTwo.toBuilder()
                .message(JoinTaskLevelTwoDto.builder()
                        .message("hello from ChildTaskLevelTwo: " + executionContext.getInputMessageOrThrow().getMessage())
                        .build())
                .build();
        distributedTaskService.setJoinMessageToBranch(joinTaskLevelTwo);

        //in order not to be in branch of current join hierarchy and run independent task or branch of flow
        distributedTaskService.scheduleFork(
                PrivateTaskDefinitions.CHILD_TASK_LEVEL_THREE_INDEPENDENT,
                executionContext.withNewMessage(SimpleMessageDto.builder()
                        .message("independent task")
                        .build()
                )
        );

        log.info("execute(): stop task: {}", executionContext.getInputMessageOrThrow().getMessage());
    }
}

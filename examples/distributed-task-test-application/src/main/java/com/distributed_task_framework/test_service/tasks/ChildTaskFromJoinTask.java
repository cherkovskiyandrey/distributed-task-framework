package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelOne;
import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class ChildTaskFromJoinTask extends BaseTask<SimpleMessageDto> {
    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<SimpleMessageDto> getDef() {
        return PrivateTaskDefinitions.CHILD_FROM_JOIN_TASK;
    }

    @Override
    public void execute(ExecutionContext<SimpleMessageDto> executionContext) throws Exception {
        log.info("execute(): start task: {}", executionContext.getInputMessageOrThrow().getMessage());
        //add data to join task level one
        JoinTaskMessage<JoinTaskLevelOne> joinMessagesLevelOne = distributedTaskService.getJoinMessagesFromBranch(PrivateTaskDefinitions.JOIN_TASK_LEVEL_ONE).get(0);

        joinMessagesLevelOne = joinMessagesLevelOne.toBuilder()
                .message(JoinTaskLevelOne.builder()
                        .message("hello from ChildTaskFromJoinTask: " + executionContext.getInputMessageOrThrow().getMessage())
                        .build())
                .build();

        distributedTaskService.setJoinMessageToBranch(joinMessagesLevelOne);

        log.info("execute(): stop task: {}", executionContext.getInputMessageOrThrow().getMessage());
    }
}

package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelTwoDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;

import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.CHILD_FROM_JOIN_TASK;
import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.JOIN_TASK_LEVEL_TWO;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class JoinTaskLevelTwo extends BaseTask<JoinTaskLevelTwoDto> {
    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<JoinTaskLevelTwoDto> getDef() {
        return JOIN_TASK_LEVEL_TWO;
    }

    @Override
    public void execute(ExecutionContext<JoinTaskLevelTwoDto> executionContext) throws Exception {
        log.info("execute(): start task");
        executionContext.getInputJoinTaskMessages()
                .forEach(joinTaskLevelOne -> log.info("execute(): {}", joinTaskLevelOne.getMessage()));

        distributedTaskService.schedule(
                CHILD_FROM_JOIN_TASK,
                executionContext.withNewMessage(SimpleMessageDto.builder()
                        .message("hello")
                        .build()
                )
        );
        log.info("execute(): stop task");
    }
}

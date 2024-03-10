package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class ChildTaskLevelThreeIndependent extends BaseTask<SimpleMessageDto> {

    @Override
    public TaskDef<SimpleMessageDto> getDef() {
        return PrivateTaskDefinitions.CHILD_TASK_LEVEL_THREE_INDEPENDENT;
    }

    @Override
    public void execute(ExecutionContext<SimpleMessageDto> executionContext) throws Exception {
        log.info("execute(): start task: {}", executionContext.getInputMessageOrThrow().getMessage());
        log.info("execute(): stop task: {}", executionContext.getInputMessageOrThrow().getMessage());
    }
}

package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelOne;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;

import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.JOIN_TASK_LEVEL_ONE;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class JoinTask extends BaseTask<JoinTaskLevelOne> {

    @Override
    public TaskDef<JoinTaskLevelOne> getDef() {
        return JOIN_TASK_LEVEL_ONE;
    }

    @Override
    public void execute(ExecutionContext<JoinTaskLevelOne> executionContext) throws Exception {
        log.info("execute(): start");
        executionContext.getInputJoinTaskMessages()
                .forEach(joinTaskLevelOne -> log.info("execute(): {}", joinTaskLevelOne.getMessage()));
        log.info("execute(): stop");
    }
}

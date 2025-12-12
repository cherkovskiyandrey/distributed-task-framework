package com.distributed_task_framework.test.autoconfigure.tasks;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.task.Task;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class DefaultTask implements Task<Void> {
    public static final TaskDef<Void> TASK_DEF = TaskDef.privateTaskDef("default", Void.class);

    @Override
    public TaskDef<Void> getDef() {
        return TASK_DEF;
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) {
    }

    @Override
    public void onFailure(FailedExecutionContext<Void> failedExecutionContext) {
    }
}

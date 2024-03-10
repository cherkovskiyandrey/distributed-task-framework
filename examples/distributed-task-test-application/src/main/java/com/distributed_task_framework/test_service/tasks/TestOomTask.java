package com.distributed_task_framework.test_service.tasks;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
@Component
public class TestOomTask implements Task<Void> {

    @Override
    public TaskDef<Void> getDef() {
        return PrivateTaskDefinitions.TEST_OOM_TASK;
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) throws Exception {
        byte[] bytes = new byte[2_000_000_000];
        System.out.println(bytes.length);
    }

    @Override
    public void onFailure(FailedExecutionContext<Void> failedExecutionContext) {
    }
}

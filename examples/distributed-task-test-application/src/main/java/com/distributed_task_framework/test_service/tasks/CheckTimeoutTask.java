package com.distributed_task_framework.test_service.tasks;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;

import java.util.concurrent.TimeUnit;

import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.CHECK_TIMEOUT_TASK;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class CheckTimeoutTask extends BaseTask<Void> {

    @Override
    public TaskDef<Void> getDef() {
        return CHECK_TIMEOUT_TASK;
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) throws Exception {
        TimeUnit.MINUTES.sleep(1);
    }

    @Override
    public boolean onFailureWithResult(FailedExecutionContext<Void> failedExecutionContext) {
        log.error("onFailureWithResult(): task has been interrupted!");
        return true;
    }
}

package com.distributed_task_framework.test.autoconfigure.tasks;

import com.distributed_task_framework.autoconfigure.annotation.TaskFixedRetryPolicy;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.utils.Signaller;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@TaskFixedRetryPolicy(delay = "PT1S", number = 600)
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class NotInterruptableTask implements Task<Void> {
    public static final TaskDef<Void> TASK_DEF = TaskDef.privateTaskDef("not-interruptable-example");

    Signaller signaller;

    @Override
    public TaskDef<Void> getDef() {
        return TASK_DEF;
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) throws Exception {
        signaller.getCyclicBarrierRef().get().await();
        TimeUnit.MINUTES.sleep(10);
    }
}

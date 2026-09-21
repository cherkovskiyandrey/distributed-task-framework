package com.distributed_task_framework.autoconfigure.tasks;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.autoconfigure.annotation.TaskCreationInterceptors;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionInterceptors;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.task.Task;
import org.springframework.stereotype.Component;

@Component
@TaskCreationInterceptors(excludedCommon = {
    TestCreationInterceptor.class
})
@TaskExecutionInterceptors(excludedCommon = {
    TestExecutionInterceptor.class
})
public class TaskWithExcludedCommonInterceptors implements Task<Void> {

    @Override
    public TaskDef<Void> getDef() {
        return TaskDef.privateTaskDef("task_with_excluded_common_interceptors", Void.class);
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) throws Exception {
    }

    @Override
    public void onFailure(FailedExecutionContext<Void> failedExecutionContext) {
    }
}

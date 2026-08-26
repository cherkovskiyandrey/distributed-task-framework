package com.distributed_task_framework.autoconfigure.tasks;

import com.distributed_task_framework.interceptor.CommonTaskCreationInterceptor;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;

public class TestCreationInterceptorThree implements CommonTaskCreationInterceptor {

    @Override
    public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
        return executionContext;
    }
}

package com.distributed_task_framework.autoconfigure.tasks;

import com.distributed_task_framework.interceptor.CommonTaskExecutionInterceptor;
import com.distributed_task_framework.interceptor.TaskExecutionChain;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;

public class TestExecutionInterceptor implements CommonTaskExecutionInterceptor {

    @Override
    public <U> void execute(ExecutionContext<U> executionContext, TaskDef<U> taskDef, TaskExecutionChain chain) throws Exception {
        chain.proceed();
    }
}

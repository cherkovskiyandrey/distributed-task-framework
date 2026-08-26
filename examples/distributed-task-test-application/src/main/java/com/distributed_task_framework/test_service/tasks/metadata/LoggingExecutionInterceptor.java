package com.distributed_task_framework.test_service.tasks.metadata;

import com.distributed_task_framework.interceptor.TaskExecutionChain;
import com.distributed_task_framework.interceptor.TaskExecutionInterceptor;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Logs metadata around execution and measures execution time
 * for every task it is attached to (see MetadataExampleTask).
 */
@Slf4j
@Component
public class LoggingExecutionInterceptor implements TaskExecutionInterceptor {

    @Override
    public <U> void execute(ExecutionContext<U> executionContext, TaskDef<U> taskDef, TaskExecutionChain chain) throws Exception {
        long start = System.nanoTime();
        log.info("execution started: taskName=[{}], taskId=[{}], metadata=[{}]",
            taskDef.getTaskName(),
            executionContext.getCurrentTaskId(),
            executionContext.getMetadata()
        );
        try {
            chain.proceed();
        } finally {
            log.info("execution finished: taskId=[{}], took=[{}ms]",
                executionContext.getCurrentTaskId(),
                (System.nanoTime() - start) / 1_000_000
            );
        }
    }
}

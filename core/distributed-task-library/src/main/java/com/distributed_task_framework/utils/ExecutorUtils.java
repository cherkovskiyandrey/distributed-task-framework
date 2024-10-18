package com.distributed_task_framework.utils;

import org.springframework.scheduling.support.DelegatingErrorHandlingRunnable;
import org.springframework.scheduling.support.TaskUtils;

public class ExecutorUtils {
    private ExecutorUtils() {
    }

    public static DelegatingErrorHandlingRunnable wrapRepeatableRunnable(Runnable runnable) {
        return TaskUtils.decorateTaskWithErrorHandler(runnable, null, true);
    }

    public static DelegatingErrorHandlingRunnable wrapRunnable(Runnable runnable) {
        return TaskUtils.decorateTaskWithErrorHandler(runnable, null, false);
    }
}

package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test_service.exceptions.SagaException;
import com.distributed_task_framework.test_service.models.SagaRevertContext;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;

@Slf4j
@Value
@Builder
public class SagaRevertTask implements Task<SagaRevertContext> {
    SagaHelper sagaHelper;
    //---
    TaskDef<SagaRevertContext> taskDef;
    Method method;
    Object bean;

    @Override
    public TaskDef<SagaRevertContext> getDef() {
        return taskDef;
    }

    @Override
    public void execute(ExecutionContext<SagaRevertContext> executionContext) throws Exception {
        SagaRevertContext sagaRevertRootContext = executionContext.getInputMessageOrThrow();
        byte[] rootArgument = sagaRevertRootContext.getSerializedArg();

        var argTotal = method.getParameters().length;
        if (argTotal != 1) {
            throw new SagaException(
                    "Unexpected number of arguments: expected 1, but passed %s".formatted(argTotal)
            );
        }
        ReflectionUtils.invokeMethod(
                method,
                bean,
                sagaHelper.toMethodArgTypedObject(rootArgument, method.getParameters()[0])
        );
    }

    //todo: process SagaException as unrecoverable
    @Override
    public void onFailure(FailedExecutionContext<SagaRevertContext> failedExecutionContext) {
        log.error("onFailure(): saga revert error failedExecutionContext=[{}], failures=[{}], isLastAttempt=[{}]",
                failedExecutionContext,
                failedExecutionContext.getFailures(),
                failedExecutionContext.isLastAttempt(),
                failedExecutionContext.getError()
        );
    }
}

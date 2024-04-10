package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.exceptions.SagaException;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.models.SagaRevertBuilderContext;
import com.distributed_task_framework.test_service.models.SagaRevertContext;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

//todo
@Slf4j
@Value
@Builder
class SagaTask implements Task<SagaContext> {
    DistributedTaskService distributedTaskService;
    TaskSerializer taskSerializer;
    SagaHelper sagaHelper;
    //---
    Map<TaskDef<SagaRevertContext>, Method> revertTaskToMethod;
    TaskDef<SagaContext> taskDef;
    Method method;
    Object bean;
    SagaMethod sagaMethodAnnotation;

    @Override
    public TaskDef<SagaContext> getDef() {
        return taskDef;
    }

    @Override
    public void execute(ExecutionContext<SagaContext> executionContext) throws Exception {
        SagaContext sagaRootContext = executionContext.getInputMessageOrThrow();

        byte[] rootArgument = sagaRootContext.getSerializedArg();
        byte[] parentArgument = parentArgumentFormContext(executionContext);
        byte[] singleArgument = ObjectUtils.firstNonNull(rootArgument, parentArgument);

        var argTotal = method.getParameters().length;
        Object result = switch (argTotal) {
            case 0 -> ReflectionUtils.invokeMethod(method, bean);
            case 1 -> ReflectionUtils.invokeMethod(
                    method,
                    bean,
                    sagaHelper.toMethodArgTypedObject(singleArgument, method.getParameters()[0])
            );
            case 2 -> ReflectionUtils.invokeMethod(
                    method,
                    bean,
                    sagaHelper.toMethodArgTypedObject(parentArgument, method.getParameters()[0]),
                    sagaHelper.toMethodArgTypedObject(rootArgument, method.getParameters()[1])
            );
            default -> throw new SagaException(
                    "Unexpected number of arguments: expected < 3, but passed %s".formatted(argTotal)
            );
        };

        TaskDef<SagaContext> nextOperationTask = sagaRootContext.getNextOperationTaskDef();
        if (nextOperationTask == null) {
            return; //last task in sequence
        }

        var sagaRevertBuilderContexts = sagaRevertBuilderContextsFromContext(executionContext);
        var sagaContextBuilder = SagaContext.builder()
                .serializedArg(result != null ? taskSerializer.writeValue(result) : null)
                .sagaRevertBuilderContexts(sagaRevertBuilderContexts);

        var currentSagaRevertBuilderContext = sagaRootContext.getCurrentSagaRevertBuilderContext();
        if (currentSagaRevertBuilderContext != null) {
            var sagaRevertBuilderContext = sagaHelper.addToSagaRevert(
                    currentSagaRevertBuilderContext,
                    parentArgument,
                    result,
                    null
            );
            sagaContextBuilder.sagaRevertBuilderContext(sagaRevertBuilderContext);
        }

        var nextTaskJoinMessage = distributedTaskService.getJoinMessagesFromBranch(nextOperationTask)
                .get(0); //we get only next level task
        nextTaskJoinMessage = nextTaskJoinMessage.toBuilder()
                .message(sagaContextBuilder.build())
                .build();
        distributedTaskService.setJoinMessageToBranch(nextTaskJoinMessage);
    }

    @SneakyThrows
    @Override
    public boolean onFailureWithResult(FailedExecutionContext<SagaContext> failedExecutionContext) {
        Throwable throwable = failedExecutionContext.getError();
        boolean isNoRetryException = Arrays.stream(sagaMethodAnnotation.noRetryFor())
                .map(thrCls -> ExceptionUtils.throwableOfType(throwable, thrCls))
                .anyMatch(Objects::nonNull)
                || ExceptionUtils.throwableOfType(throwable, SagaException.class) != null;
        boolean isLastAttempt = failedExecutionContext.isLastAttempt() || isNoRetryException;

        if (isLastAttempt) {
            buildAndScheduleRevertSagaPipeline(failedExecutionContext);
        }

//                                    //todo: logs
//                                    log.error(
//                                            "onFailureWithResult(): isNoRetryException=[{}], failedExecutionContext=[{}], " +
//                                                    "failures=[{}], isLastAttempt=[{}]",
//                                            failedExecutionContext,
//                                            failedExecutionContext.getFailures(),
//                                            failedExecutionContext.isLastAttempt(),
//                                            failedExecutionContext.getError()
//                                    );

        return isNoRetryException;
    }

    private void buildAndScheduleRevertSagaPipeline(FailedExecutionContext<SagaContext> failedExecutionContext) throws Exception {
        Throwable throwable = failedExecutionContext.getError();
        SagaContext sagaRootContext = failedExecutionContext.getInputMessageOrThrow();
        var sagaRevertBuilderContexts = sagaRevertBuilderContextsFromContext(failedExecutionContext);

        TaskId prevRevertTaskId = null;
        var currentSagaRevertBuilderContext = sagaRootContext.getCurrentSagaRevertBuilderContext();
        if (currentSagaRevertBuilderContext != null) {
            byte[] parentArgumentSer = parentArgumentFormContext(failedExecutionContext);
            Object parentArgument = parentArgumentSer != null ?
                    sagaHelper.toMethodArgTypedObject(parentArgumentSer, method.getParameters()[0])
                    : null;
            var sagaRevertBuilderContext = sagaHelper.addToSagaRevert(
                    currentSagaRevertBuilderContext,
                    parentArgument,
                    null,
                    throwable
            );
            prevRevertTaskId = distributedTaskService.schedule(
                    sagaRevertBuilderContext.getRevertOperationTaskDef(),
                    failedExecutionContext.withNewMessage(SagaRevertContext.builder()
                            .serializedArg(sagaRevertBuilderContext.getSerializedArg())
                            .build()
                    )
            );
        }

        for (var sagaRevertBuilderContext : Lists.reverse(sagaRevertBuilderContexts)) {
            if (prevRevertTaskId == null) {
                prevRevertTaskId = distributedTaskService.schedule(
                        sagaRevertBuilderContext.getRevertOperationTaskDef(),
                        failedExecutionContext.withNewMessage(SagaRevertContext.builder()
                                .serializedArg(sagaRevertBuilderContext.getSerializedArg())
                                .build()
                        )
                );
            } else {
                prevRevertTaskId = distributedTaskService.scheduleJoin(
                        sagaRevertBuilderContext.getRevertOperationTaskDef(),
                        failedExecutionContext.withNewMessage(SagaRevertContext.builder()
                                .serializedArg(sagaRevertBuilderContext.getSerializedArg())
                                .build()
                        ),
                        List.of(prevRevertTaskId)
                );
            }
        }
    }

    private byte[] parentArgumentFormContext(ExecutionContext<SagaContext> executionContext) {
        return executionContext.getInputJoinTaskMessages().stream()
                .map(SagaContext::getSerializedArg)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    private List<SagaRevertBuilderContext> sagaRevertBuilderContextsFromContext(ExecutionContext<SagaContext> executionContext) {
        return executionContext.getInputJoinTaskMessages().stream()
                .map(SagaContext::getSagaRevertBuilderContexts)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(List.of());
    }
}

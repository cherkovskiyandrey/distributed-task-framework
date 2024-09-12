package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.annotations.SagaMethod;
import com.distributed_task_framework.saga.exceptions.SagaInternalException;
import com.distributed_task_framework.saga.models.SagaEmbeddedActionContext;
import com.distributed_task_framework.saga.models.SagaEmbeddedPipelineContext;
import com.distributed_task_framework.saga.services.SagaContextService;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.utils.ArgumentProvider;
import com.distributed_task_framework.saga.utils.ArgumentProviderBuilder;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.task.Task;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;


@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SagaTask implements Task<SagaEmbeddedPipelineContext> {
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaContextService sagaContextService;
    TaskSerializer taskSerializer;
    SagaHelper sagaHelper;
    TaskDef<SagaEmbeddedPipelineContext> taskDef;
    Method method;
    Object bean;
    SagaMethod sagaMethodAnnotation;

    @Override
    public TaskDef<SagaEmbeddedPipelineContext> getDef() {
        return taskDef;
    }

    @Override
    public void execute(ExecutionContext<SagaEmbeddedPipelineContext> executionContext) throws Exception {
        SagaEmbeddedPipelineContext sagaEmbeddedPipelineContext = executionContext.getInputMessageOrThrow();
        SagaEmbeddedActionContext currentSagaEmbeddedActionContext = sagaEmbeddedPipelineContext.getCurrentSagaContext();
        SagaSchemaArguments operationSagaSchemaArguments = currentSagaEmbeddedActionContext.getOperationSagaSchemaArguments();

        byte[] rootArgument = sagaEmbeddedPipelineContext.getRootSagaContext().getSerializedInput();
        byte[] parentArgument = sagaEmbeddedPipelineContext.getParentSagaContext()
                .flatMap(sagaContext -> Optional.ofNullable(sagaContext.getSerializedOutput()))
                .orElse(null);
        ArgumentProviderBuilder argumentProviderBuilder = new ArgumentProviderBuilder(operationSagaSchemaArguments);
        argumentProviderBuilder.reg(SagaArguments.ROOT_INPUT, rootArgument);
        argumentProviderBuilder.reg(SagaArguments.PARENT_OUTPUT, parentArgument);
        ArgumentProvider argumentProvider = argumentProviderBuilder.build();

        var argTotal = method.getParameters().length;
        if (argTotal != argumentProvider.size()) {
            throw new SagaInternalException(
                    "Unexpected number of arguments: expected=%d, but passed=%d".formatted(argumentProvider.size(), argTotal)
            );
        }

        Object result = switch (argTotal) {
            case 1 -> ReflectionUtils.invokeMethod(
                    method,
                    bean,
                    sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0])
            );
            case 2 -> ReflectionUtils.invokeMethod(
                    method,
                    bean,
                    sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0]),
                    sagaHelper.toMethodArgTypedObject(argumentProvider.getById(1), method.getParameters()[1])
            );
            default -> throw new SagaInternalException(
                    "Unexpected number of arguments: expected=%d, but passed=%d".formatted(argumentProvider.size(), argTotal)
            );
        };

        if (!sagaEmbeddedPipelineContext.hasNext()) {
            Class<?> returnType = method.getReturnType();
            if (!sagaHelper.isVoidType(returnType)) {
                sagaContextService.setOkResult(
                        sagaEmbeddedPipelineContext.getSagaId(),
                        taskSerializer.writeValue(result)
                );
            } else {
                sagaContextService.setCompleted(sagaEmbeddedPipelineContext.getSagaId());
            }
            return; //last task in sequence
        }

        currentSagaEmbeddedActionContext = currentSagaEmbeddedActionContext.toBuilder()
                .serializedOutput(taskSerializer.writeValue(result))
                .build();
        sagaEmbeddedPipelineContext.setCurrentSagaContext(currentSagaEmbeddedActionContext);

        sagaEmbeddedPipelineContext.moveToNext();
        var nextSagaContext = sagaEmbeddedPipelineContext.getCurrentSagaContext();
        distributedTaskService.schedule(
                sagaRegister.resolveByTaskName(nextSagaContext.getSagaMethodTaskName()),
                executionContext.withNewMessage(sagaEmbeddedPipelineContext)
        );
        sagaContextService.track(sagaEmbeddedPipelineContext);
    }

    @SneakyThrows
    @Override
    public boolean onFailureWithResult(FailedExecutionContext<SagaEmbeddedPipelineContext> failedExecutionContext) {
        Throwable exception = failedExecutionContext.getError();
        boolean isNoRetryException = Arrays.stream(sagaMethodAnnotation.noRetryFor())
                .map(thrCls -> ExceptionUtils.throwableOfType(exception, thrCls))
                .anyMatch(Objects::nonNull)
                || ExceptionUtils.throwableOfType(exception, SagaInternalException.class) != null;
        boolean isLastAttempt = failedExecutionContext.isLastAttempt() || isNoRetryException;

        log.error("onFailureWithResult(): saga operation error failedExecutionContext=[{}], failures=[{}], isLastAttempt=[{}]",
                failedExecutionContext,
                failedExecutionContext.getFailures(),
                isLastAttempt,
                failedExecutionContext.getError()
        );

        if (isLastAttempt) {
            SagaEmbeddedPipelineContext sagaEmbeddedPipelineContext = failedExecutionContext.getInputMessageOrThrow();
            SagaEmbeddedActionContext currentSagaEmbeddedActionContext = sagaEmbeddedPipelineContext.getCurrentSagaContext();

            JavaType exceptionType = TypeFactory.defaultInstance().constructType(exception.getClass());

            //reset stack trace
            exception.setStackTrace(new StackTraceElement[0]);
            byte[] serializedException = taskSerializer.writeValue(exception);

            sagaContextService.setFailResult(
                    sagaEmbeddedPipelineContext.getSagaId(),
                    serializedException,
                    exceptionType
            );

            currentSagaEmbeddedActionContext = currentSagaEmbeddedActionContext.toBuilder()
                    .exceptionType(exceptionType.toCanonical())
                    .serializedException(serializedException)
                    .build();
            sagaEmbeddedPipelineContext.setCurrentSagaContext(currentSagaEmbeddedActionContext);
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            if (!sagaEmbeddedPipelineContext.hasNext()) {
                return true;
            }

            sagaEmbeddedPipelineContext.moveToNext();
            currentSagaEmbeddedActionContext = sagaEmbeddedPipelineContext.getCurrentSagaContext();
            distributedTaskService.schedule(
                    sagaRegister.resolveByTaskName(currentSagaEmbeddedActionContext.getSagaRevertMethodTaskName()),
                    failedExecutionContext.withNewMessage(sagaEmbeddedPipelineContext)
            );
            sagaContextService.track(sagaEmbeddedPipelineContext);
        }

        return isLastAttempt;
    }
}

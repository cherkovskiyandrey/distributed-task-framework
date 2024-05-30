package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.services.SagaRegister;
import com.distributed_task_framework.test_service.utils.SagaArguments;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.exceptions.SagaInternalException;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.models.SagaPipelineContext;
import com.distributed_task_framework.test_service.utils.ArgumentProvider;
import com.distributed_task_framework.test_service.utils.ArgumentProviderBuilder;
import com.distributed_task_framework.test_service.utils.SagaSchemaArguments;
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
public class SagaTask implements Task<SagaPipelineContext> {
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    TaskSerializer taskSerializer;
    SagaHelper sagaHelper;
    TaskDef<SagaPipelineContext> taskDef;
    Method method;
    Object bean;
    SagaMethod sagaMethodAnnotation;

    @Override
    public TaskDef<SagaPipelineContext> getDef() {
        return taskDef;
    }

    @Override
    public void execute(ExecutionContext<SagaPipelineContext> executionContext) throws Exception {
        SagaPipelineContext sagaPipelineContext = executionContext.getInputMessageOrThrow();
        SagaContext currentSagaContext = sagaPipelineContext.getCurrentSagaContext();
        SagaSchemaArguments operationSagaSchemaArguments = currentSagaContext.getOperationSagaSchemaArguments();

        byte[] rootArgument = sagaPipelineContext.getRootSagaContext().getSerializedInput();
        byte[] parentArgument = sagaPipelineContext.getParentSagaContext()
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

        if (!sagaPipelineContext.hasNext()) {
            return; //last task in sequence
        }

        currentSagaContext = currentSagaContext.toBuilder()
                .serializedOutput(taskSerializer.writeValue(result))
                .build();
        sagaPipelineContext.setCurrentSagaContext(currentSagaContext);

        sagaPipelineContext.moveToNext();
        var nextSagaContext = sagaPipelineContext.getCurrentSagaContext();
        distributedTaskService.schedule(
                sagaRegister.resolveByTaskName(nextSagaContext.getSagaMethodTaskName()),
                executionContext.withNewMessage(sagaPipelineContext)
        );
    }

    @SneakyThrows
    @Override
    public boolean onFailureWithResult(FailedExecutionContext<SagaPipelineContext> failedExecutionContext) {
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
            SagaPipelineContext sagaPipelineContext = failedExecutionContext.getInputMessageOrThrow();
            SagaContext currentSagaContext = sagaPipelineContext.getCurrentSagaContext();

            JavaType exceptionType = TypeFactory.defaultInstance().constructType(exception.getClass());

            //reset stack trace
            exception.setStackTrace(new StackTraceElement[0]);
            currentSagaContext = currentSagaContext.toBuilder()
                    .exceptionType(exceptionType.toCanonical())
                    .serializedException(taskSerializer.writeValue(exception))
                    .build();
            sagaPipelineContext.setCurrentSagaContext(currentSagaContext);
            sagaPipelineContext.rewindToRevertFormCurrentPosition();
            if (!sagaPipelineContext.hasNext()) {
                return true;
            }

            sagaPipelineContext.moveToNext();
            currentSagaContext = sagaPipelineContext.getCurrentSagaContext();
            distributedTaskService.schedule(
                    sagaRegister.resolveByTaskName(currentSagaContext.getSagaRevertMethodTaskName()),
                    failedExecutionContext.withNewMessage(sagaPipelineContext)
            );
        }

        return isLastAttempt;
    }
}

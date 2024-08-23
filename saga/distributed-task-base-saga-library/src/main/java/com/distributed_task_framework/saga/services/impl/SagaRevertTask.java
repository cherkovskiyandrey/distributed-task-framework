package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.saga.exceptions.SagaInternalException;
import com.distributed_task_framework.saga.models.SagaEmbeddedActionContext;
import com.distributed_task_framework.saga.models.SagaEmbeddedPipelineContext;
import com.distributed_task_framework.saga.utils.ArgumentProvider;
import com.distributed_task_framework.saga.utils.ArgumentProviderBuilder;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SagaRevertTask implements Task<SagaEmbeddedPipelineContext> {
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaHelper sagaHelper;
    TaskDef<SagaEmbeddedPipelineContext> taskDef;
    Method method;
    Object bean;

    @Override
    public TaskDef<SagaEmbeddedPipelineContext> getDef() {
        return taskDef;
    }

    @Override
    public void execute(ExecutionContext<SagaEmbeddedPipelineContext> executionContext) throws Exception {
        SagaEmbeddedPipelineContext sagaEmbeddedPipelineContext = executionContext.getInputMessageOrThrow();
        SagaEmbeddedActionContext currentSagaEmbeddedActionContext = sagaEmbeddedPipelineContext.getCurrentSagaContext();
        SagaSchemaArguments sagaSchemaArguments = currentSagaEmbeddedActionContext.getRevertOperationSagaSchemaArguments();

        byte[] serializedInput = sagaEmbeddedPipelineContext.getRootSagaContext().getSerializedInput();
        byte[] serializedOutput = currentSagaEmbeddedActionContext.getSerializedOutput();
        byte[] parentSerializedOutput = sagaEmbeddedPipelineContext.getParentSagaContext()
                .flatMap(sagaContext -> Optional.ofNullable(sagaContext.getSerializedOutput()))
                .orElse(null);
        var exceptionType = currentSagaEmbeddedActionContext.getExceptionType();
        byte[] serializedException = currentSagaEmbeddedActionContext.getSerializedException();
        var sagaExecutionException = sagaHelper.buildExecutionException(exceptionType, serializedException);

        ArgumentProviderBuilder argumentProviderBuilder = new ArgumentProviderBuilder(sagaSchemaArguments);
        argumentProviderBuilder.reg(SagaArguments.ROOT_INPUT, serializedInput);
        argumentProviderBuilder.reg(SagaArguments.OUTPUT, serializedOutput);
        argumentProviderBuilder.reg(SagaArguments.PARENT_OUTPUT, parentSerializedOutput);
        argumentProviderBuilder.reg(SagaArguments.THROWABLE, sagaExecutionException);
        ArgumentProvider argumentProvider = argumentProviderBuilder.build();

        var argTotal = method.getParameters().length;
        if (argTotal != argumentProvider.size()) {
            throw new SagaInternalException(
                    "Unexpected number of arguments: expected=%d, but passed=%d".formatted(argumentProvider.size(), argTotal)
            );
        }

        switch (argTotal) {
            case 2 -> ReflectionUtils.invokeMethod(
                    method,
                    bean,
                    sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0]),
                    argumentProvider.getById(1)
            );
            case 3 -> ReflectionUtils.invokeMethod(
                    method,
                    bean,
                    sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0]),
                    sagaHelper.toMethodArgTypedObject(argumentProvider.getById(1), method.getParameters()[1]),
                    argumentProvider.getById(2)
            );
            case 4 -> ReflectionUtils.invokeMethod(
                    method,
                    bean,
                    sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0]),
                    sagaHelper.toMethodArgTypedObject(argumentProvider.getById(1), method.getParameters()[1]),
                    sagaHelper.toMethodArgTypedObject(argumentProvider.getById(2), method.getParameters()[2]),
                    argumentProvider.getById(3)
            );
        }

        scheduleNextRevertIfRequired(sagaEmbeddedPipelineContext, executionContext);
    }

    @Override
    public boolean onFailureWithResult(FailedExecutionContext<SagaEmbeddedPipelineContext> failedExecutionContext) {
        Throwable throwable = failedExecutionContext.getError();
        boolean isNoRetryException = ExceptionUtils.throwableOfType(throwable, SagaInternalException.class) != null;
        boolean isLastAttempt = failedExecutionContext.isLastAttempt() || isNoRetryException;

        log.error("onFailureWithResult(): saga revert operation error failedExecutionContext=[{}], failures=[{}], isLastAttempt=[{}]",
                failedExecutionContext,
                failedExecutionContext.getFailures(),
                isLastAttempt,
                failedExecutionContext.getError()
        );

        if (isLastAttempt) {
            scheduleNextRevertIfRequired(failedExecutionContext.getInputMessageOrThrow(), failedExecutionContext);
        }

        return isLastAttempt;
    }

    @SneakyThrows
    private void scheduleNextRevertIfRequired(SagaEmbeddedPipelineContext sagaEmbeddedPipelineContext,
                                              ExecutionContext<SagaEmbeddedPipelineContext> executionContext) {
        if (!sagaEmbeddedPipelineContext.hasNext()) {
            log.info(
                    "scheduleNextRevertIfRequired(): revert chain has been completed for sagaPipelineContext with id=[{}]",
                    sagaEmbeddedPipelineContext.getSagaId()
            );
            return;
        }

        sagaEmbeddedPipelineContext.moveToNext();
        var currentSagaContext = sagaEmbeddedPipelineContext.getCurrentSagaContext();
        distributedTaskService.schedule(
                sagaRegister.resolveByTaskName(currentSagaContext.getSagaRevertMethodTaskName()),
                executionContext.withNewMessage(sagaEmbeddedPipelineContext)
        );
    }
}

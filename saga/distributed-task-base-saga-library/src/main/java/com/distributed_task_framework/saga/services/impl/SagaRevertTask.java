package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.exceptions.SagaInternalException;
import com.distributed_task_framework.saga.models.Saga;
import com.distributed_task_framework.saga.models.SagaAction;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.utils.ArgumentProvider;
import com.distributed_task_framework.saga.utils.ArgumentProviderBuilder;
import com.distributed_task_framework.saga.utils.ReflectionHelper;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.task.Task;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.Method;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SagaRevertTask implements Task<SagaPipeline> {
    SagaResolver sagaResolver;
    DistributedTaskService distributedTaskService;
    SagaManager sagaManager;
    SagaHelper sagaHelper;
    TaskDef<SagaPipeline> taskDef;
    Method method;
    Object bean;

    @Override
    public TaskDef<SagaPipeline> getDef() {
        return taskDef;
    }

    @Override
    public void execute(ExecutionContext<SagaPipeline> executionContext) throws Exception {
        SagaPipeline sagaPipeline = executionContext.getInputMessageOrThrow();
        var sagaId = sagaPipeline.getSagaId();
        var sagaOpt = sagaManager.getIfExists(sagaId);
        if (sagaOpt.isEmpty()) {
            log.warn("execute(): sagaId=[{}] doesn't exists, stop execution.", sagaId);
            return;
        }

        var saga = sagaOpt.get();
        if (saga.isCompleted()) {
            log.info("execute(): sagaId=[{}] has been completed, stop execution of revert.", sagaId);
            return;
        }

        SagaAction currentSagaAction = sagaPipeline.getCurrentAction();
        SagaSchemaArguments sagaSchemaArguments = currentSagaAction.getRevertOperationSagaSchemaArguments();

        byte[] serializedInput = sagaPipeline.getRootAction().getSerializedInput();
        byte[] serializedOutput = currentSagaAction.getSerializedOutput();
        byte[] parentSerializedOutput = sagaPipeline.getParentAction()
            .flatMap(sagaContext -> Optional.ofNullable(sagaContext.getSerializedOutput()))
            .orElse(null);
        var exceptionType = currentSagaAction.getExceptionType();
        byte[] serializedException = currentSagaAction.getSerializedException();
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
            case 2 -> ReflectionHelper.invokeMethod(
                method,
                bean,
                sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0]),
                argumentProvider.getById(1)
            );
            case 3 -> ReflectionHelper.invokeMethod(
                method,
                bean,
                sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0]),
                sagaHelper.toMethodArgTypedObject(argumentProvider.getById(1), method.getParameters()[1]),
                argumentProvider.getById(2)
            );
            case 4 -> ReflectionHelper.invokeMethod(
                method,
                bean,
                sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0]),
                sagaHelper.toMethodArgTypedObject(argumentProvider.getById(1), method.getParameters()[1]),
                sagaHelper.toMethodArgTypedObject(argumentProvider.getById(2), method.getParameters()[2]),
                argumentProvider.getById(3)
            );
        }

        scheduleNextRevertIfRequired(saga, sagaPipeline, executionContext);
    }

    @Override
    public boolean onFailureWithResult(FailedExecutionContext<SagaPipeline> failedExecutionContext) throws Exception {
        var sagaId = failedExecutionContext.getInputMessageOrThrow().getSagaId();
        var sagaOpt = sagaManager.getIfExists(sagaId);
        if (sagaOpt.isEmpty()) {
            log.warn("execute(): sagaId=[{}] doesn't exists, stop execution.", sagaId);
            return true;
        }

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
            scheduleNextRevertIfRequired(
                sagaOpt.get(),
                failedExecutionContext.getInputMessageOrThrow(),
                failedExecutionContext
            );
        }

        return isLastAttempt;
    }

    private void scheduleNextRevertIfRequired(Saga saga,
                                              SagaPipeline sagaPipeline,
                                              ExecutionContext<SagaPipeline> executionContext) throws Exception {
        if (!sagaPipeline.hasNext() || saga.isStopOnFailedAnyRevert()) {
            log.info(
                "scheduleNextRevertIfRequired(): revert chain has been completed for sagaPipelineContext with id=[{}]",
                sagaPipeline.getSagaId()
            );
            sagaManager.completeIfExists(sagaPipeline.getSagaId());
            return;
        }

        sagaPipeline.moveToNext();
        var currentSagaContext = sagaPipeline.getCurrentAction();
        distributedTaskService.schedule(
            sagaResolver.resolveByTaskName(currentSagaContext.getSagaRevertMethodTaskName()),
            executionContext.withNewMessage(sagaPipeline)
        );
        sagaManager.trackIfExists(sagaPipeline);
    }
}

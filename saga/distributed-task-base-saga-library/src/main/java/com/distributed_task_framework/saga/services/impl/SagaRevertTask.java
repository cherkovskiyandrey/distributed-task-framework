package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.exceptions.SagaInternalException;
import com.distributed_task_framework.saga.models.SagaAction;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.SagaManager;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.utils.ArgumentProvider;
import com.distributed_task_framework.saga.utils.ArgumentProviderBuilder;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.task.Task;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SagaRevertTask implements Task<SagaPipeline> {
    SagaRegister sagaRegister;
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
        if (sagaManager.isCompleted(sagaId)) {
            log.info("execute(): sagaId=[{}] has been completed or shutdown, stop execution.", sagaId);
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

        scheduleNextRevertIfRequired(sagaPipeline, executionContext);
    }

    @Override
    public boolean onFailureWithResult(FailedExecutionContext<SagaPipeline> failedExecutionContext) throws Exception {
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

    private void scheduleNextRevertIfRequired(SagaPipeline sagaPipeline,
                                              ExecutionContext<SagaPipeline> executionContext) throws Exception {
        if (!sagaPipeline.hasNext()) {
            log.info(
                "scheduleNextRevertIfRequired(): revert chain has been completed for sagaPipelineContext with id=[{}]",
                sagaPipeline.getSagaId()
            );
            sagaManager.complete(sagaPipeline.getSagaId());
            return;
        }

        sagaPipeline.moveToNext();
        var currentSagaContext = sagaPipeline.getCurrentAction();
        distributedTaskService.schedule(
            sagaRegister.resolveByTaskName(currentSagaContext.getSagaRevertMethodTaskName()),
            executionContext.withNewMessage(sagaPipeline)
        );
        sagaManager.track(sagaPipeline);
    }
}

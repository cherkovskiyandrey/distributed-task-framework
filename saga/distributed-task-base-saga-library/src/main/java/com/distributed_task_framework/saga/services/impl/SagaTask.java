package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.annotations.SagaMethod;
import com.distributed_task_framework.saga.exceptions.SagaInternalException;
import com.distributed_task_framework.saga.exceptions.SagaInterruptedException;
import com.distributed_task_framework.saga.models.SagaAction;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import com.distributed_task_framework.saga.persistence.repository.SagaContextRepository;
import com.distributed_task_framework.saga.services.SagaManager;
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
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;


@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SagaTask implements Task<SagaPipeline> {
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaManager sagaManager;
    SagaContextRepository sagaContextRepository;
    TaskSerializer taskSerializer;
    SagaHelper sagaHelper;
    TaskDef<SagaPipeline> taskDef;
    Method method;
    Object bean;
    SagaMethod sagaMethodAnnotation;

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

        if (sagaManager.isCanceled(sagaId)) {
            log.info("execute(): sagaId=[{}] has been canceled, start interrupting...", sagaId);
            throw new SagaInterruptedException(sagaId);
        }

        SagaAction currentSagaAction = sagaPipeline.getCurrentAction();
        SagaSchemaArguments operationSagaSchemaArguments = currentSagaAction.getOperationSagaSchemaArguments();

        byte[] rootArgument = sagaPipeline.getRootAction().getSerializedInput();
        byte[] parentArgument = sagaPipeline.getParentAction()
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

        if (!sagaPipeline.hasNext()) {
            Class<?> returnType = method.getReturnType();
            if (!sagaHelper.isVoidType(returnType)) {
                sagaManager.setOkResult(sagaId, taskSerializer.writeValue(result));
            }
            sagaManager.complete(sagaId);
            return; //last task in sequence
        }

        currentSagaAction = currentSagaAction.toBuilder()
            .serializedOutput(taskSerializer.writeValue(result))
            .build();
        sagaPipeline.setCurrentAction(currentSagaAction);

        sagaPipeline.moveToNext();
        var nextSagaContext = sagaPipeline.getCurrentAction();
        distributedTaskService.schedule(
            sagaRegister.resolveByTaskName(nextSagaContext.getSagaMethodTaskName()),
            executionContext.withNewMessage(sagaPipeline)
        );
        sagaManager.track(sagaPipeline);
    }

    @Override
    public boolean onFailureWithResult(FailedExecutionContext<SagaPipeline> failedExecutionContext) throws Exception {
        SagaPipeline sagaPipeline = failedExecutionContext.getInputMessageOrThrow();
        Throwable exception = failedExecutionContext.getError();
        boolean isNoRetryException = Arrays.stream(sagaMethodAnnotation.noRetryFor())
            .map(thrCls -> ExceptionUtils.throwableOfType(exception, thrCls))
            .anyMatch(Objects::nonNull)
            || ExceptionUtils.throwableOfType(exception, SagaInternalException.class) != null;

        boolean isLastAttempt = failedExecutionContext.isLastAttempt()
            || isNoRetryException
            || ExceptionUtils.throwableOfType(exception, SagaInterruptedException.class) != null;

        log.error("onFailureWithResult(): saga operation error failedExecutionContext=[{}], failures=[{}], isLastAttempt=[{}]",
            failedExecutionContext,
            failedExecutionContext.getFailures(),
            isLastAttempt,
            failedExecutionContext.getError()
        );

        if (isLastAttempt) {
            scheduleNextRevertIfRequired(exception, failedExecutionContext);
        } else {
            var serializedException = toSerializedException(exception);
            //the idea to save exception between attempts in order to use it when meet cancellation of saga;
            //in that case we will reuse original exception instead of use SagaInterruptedException
            sagaManager.setFailResult(
                sagaPipeline.getSagaId(),
                serializedException.serializedException(),
                serializedException.exceptionType()
            );
        }

        return isLastAttempt;
    }

    private SerializedException toSerializedException(Throwable exception) throws IOException {
        JavaType exceptionType = TypeFactory.defaultInstance().constructType(exception.getClass());

        //reset stack trace
        exception.setStackTrace(new StackTraceElement[0]);
        byte[] serializedException = taskSerializer.writeValue(exception);

        return new SerializedException(serializedException, exceptionType);
    }

    private void scheduleNextRevertIfRequired(Throwable exception,
                                              FailedExecutionContext<SagaPipeline> failedExecutionContext) throws Exception {
        SagaPipeline sagaPipeline = failedExecutionContext.getInputMessageOrThrow();
        SagaAction currentSagaAction = sagaPipeline.getCurrentAction();

        boolean hasBeenInterrupted = ExceptionUtils.throwableOfType(exception, SagaInterruptedException.class) != null;
        boolean hasFailedBeforeInterrupted = hasBeenInterrupted && failedExecutionContext.getFailures() > 1;

        if (hasFailedBeforeInterrupted) {
            //reuse original exception instead of SagaInterruptedException
            SagaEntity sagaEntity = sagaContextRepository.findById(sagaPipeline.getSagaId()).orElseThrow();
            currentSagaAction = currentSagaAction.toBuilder()
                .exceptionType(sagaEntity.getExceptionType())
                .serializedException(sagaEntity.getResult())
                .build();
            sagaPipeline.setCurrentAction(currentSagaAction);
            sagaPipeline.rewindToRevertFromCurrentPosition();

        } else if (hasBeenInterrupted) {
            sagaPipeline.rewindToRevertFromPrevPosition();

        } else {
            var serializedException = toSerializedException(exception);
            sagaManager.setFailResult(
                sagaPipeline.getSagaId(),
                serializedException.serializedException(),
                serializedException.exceptionType()
            );
            currentSagaAction = currentSagaAction.toBuilder()
                .exceptionType(serializedException.exceptionType().toCanonical())
                .serializedException(serializedException.serializedException())
                .build();
            sagaPipeline.setCurrentAction(currentSagaAction);
            sagaPipeline.rewindToRevertFromCurrentPosition();
        }

        if (sagaPipeline.hasNext()) {
            sagaPipeline.moveToNext();
            currentSagaAction = sagaPipeline.getCurrentAction();
            distributedTaskService.schedule(
                sagaRegister.resolveByTaskName(currentSagaAction.getSagaRevertMethodTaskName()),
                failedExecutionContext.withNewMessage(sagaPipeline)
            );
            sagaManager.track(sagaPipeline);
        } else {
            sagaManager.complete(sagaPipeline.getSagaId());
        }
    }

    record SerializedException(
        byte[] serializedException,
        JavaType exceptionType
    ){}
}

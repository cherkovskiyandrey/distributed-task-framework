package com.distributed_task_framework.saga.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.StateHolder;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TypeDef;
import com.distributed_task_framework.saga.exceptions.SagaInternalException;
import com.distributed_task_framework.saga.models.SagaAction;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.impl.SagaHelper;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.utils.ArgumentProvider;
import com.distributed_task_framework.saga.utils.ArgumentProviderBuilder;
import com.distributed_task_framework.saga.utils.ReflectionHelper;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.task.StatefulTask;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;


@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SagaTask extends BaseSagaTask implements StatefulTask<SagaPipeline, SagaTask.SerializedException> {
    TaskSerializer taskSerializer;
    SagaHelper sagaHelper;
    TaskDef<SagaPipeline> taskDef;
    Method method;
    Object bean;
    SagaMethodSettings sagaMethodSettings;

    public SagaTask(SagaResolver sagaResolver,
                    SagaManager sagaManager,
                    DistributedTaskService distributedTaskService,
                    TaskSerializer taskSerializer,
                    SagaHelper sagaHelper,
                    TaskDef<SagaPipeline> taskDef,
                    Method method,
                    Object bean,
                    SagaMethodSettings sagaMethodSettings) {
        super(sagaResolver, sagaManager, distributedTaskService);
        this.taskSerializer = taskSerializer;
        this.sagaHelper = sagaHelper;
        this.taskDef = taskDef;
        this.method = method;
        this.bean = bean;
        this.sagaMethodSettings = sagaMethodSettings;
    }

    @Override
    public TaskDef<SagaPipeline> getDef() {
        return taskDef;
    }

    @Override
    public TypeDef<SerializedException> stateDef() {
        return TypeDef.of(SerializedException.class);
    }

    @Override
    public void execute(ExecutionContext<SagaPipeline> executionContext,
                        StateHolder<SerializedException> stateHolder) throws Exception {
        SagaPipeline sagaPipeline = executionContext.getInputMessageOrThrow();
        var sagaId = sagaPipeline.getSagaId();
        boolean isLastAction = !sagaPipeline.hasNext();

        var sagaOpt = sagaManager.getIfExists(sagaId);
        if (sagaOpt.isEmpty()) {
            log.warn("execute(): sagaId=[{}] doesn't exists, stop execution.", sagaId);
            return;
        }

        var saga = sagaOpt.get();
        if (saga.isCompleted()) {
            log.info("execute(): sagaId=[{}] has been completed, stop execution.", sagaId);
            return;
        }
        if (saga.isCanceled()) {
            log.info("execute(): sagaId=[{}] has been canceled, start interrupting...", sagaId);
            scheduleNextRevertOrCompleteBeforeExecution(executionContext, stateHolder, sagaPipeline);
            return;
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
            case 1 -> ReflectionHelper.invokeMethod(
                method,
                bean,
                sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0])
            );
            case 2 -> ReflectionHelper.invokeMethod(
                method,
                bean,
                sagaHelper.toMethodArgTypedObject(argumentProvider.getById(0), method.getParameters()[0]),
                sagaHelper.toMethodArgTypedObject(argumentProvider.getById(1), method.getParameters()[1])
            );
            default -> throw new SagaInternalException(
                "Unexpected number of arguments: expected=%d, but passed=%d".formatted(argumentProvider.size(), argTotal)
            );
        };

        sagaOpt = sagaManager.getIfExists(sagaId);
        if (sagaOpt.isEmpty()) {
            log.warn("execute(): sagaId=[{}] doesn't exists, stop execution.", sagaId);
            return;
        }
        if (sagaOpt.get().isCanceled()) {
            log.info("execute(): sagaId=[{}] has been canceled in the end of current step, scheduling interrupting...", sagaId);
            sagaPipeline.rewindToRevertFromCurrentPosition();
            scheduleNextOrComplete(executionContext, sagaPipeline, SagaAction::getSagaRevertMethodTaskName);
            return;
        }

        if (isLastAction) {
            Class<?> returnType = method.getReturnType();
            if (!sagaHelper.isVoidType(returnType)) {
                sagaManager.setOkResultIfExists(sagaId, taskSerializer.writeValue(result));
            }
            sagaManager.completeIfExists(sagaId);
            return; //last task in sequence
        }

        currentSagaAction = currentSagaAction.toBuilder()
            .serializedOutput(taskSerializer.writeValue(result))
            .build();
        sagaPipeline.setCurrentAction(currentSagaAction);

        scheduleNextOrComplete(executionContext, sagaPipeline, SagaAction::getSagaMethodTaskName);
    }

    private void scheduleNextRevertOrCompleteBeforeExecution(ExecutionContext<SagaPipeline> executionContext,
                                                             StateHolder<SerializedException> stateHolder,
                                                             SagaPipeline sagaPipeline) throws Exception {
        boolean hasFailedAttempts = executionContext.getExecutionAttempt() > 1;
        if (hasFailedAttempts) {
            var serializedExceptionOpt = stateHolder.get();
            if (serializedExceptionOpt.isPresent() && sagaPipeline.getCurrentAction().hasRevert()) {
                var serializedException = serializedExceptionOpt.get();
                SagaAction currentSagaAction = sagaPipeline.getCurrentAction().toBuilder()
                    .exceptionType(serializedException.exceptionType().toCanonical())
                    .serializedException(serializedException.serializedException())
                    .build();
                sagaPipeline.setCurrentAction(currentSagaAction);
            }
            sagaPipeline.rewindToRevertFromCurrentPosition();
        } else {
            sagaPipeline.rewindToRevertFromPrevPosition();
        }
        scheduleNextOrComplete(executionContext, sagaPipeline, SagaAction::getSagaRevertMethodTaskName);
    }

    @Override
    public boolean onFailureWithResult(FailedExecutionContext<SagaPipeline> failedExecutionContext,
                                       StateHolder<SerializedException> stateHolder) throws Exception {
        SagaPipeline sagaPipeline = failedExecutionContext.getInputMessageOrThrow();
        Throwable exception = failedExecutionContext.getError();
        boolean isNoRetryException = sagaMethodSettings.getNoRetryFor().stream()
            .map(thrCls -> ExceptionUtils.throwableOfType(exception, thrCls))
            .anyMatch(Objects::nonNull);

        boolean isLastAttempt = failedExecutionContext.isLastAttempt() || isNoRetryException;
        log.error("onFailureWithResult(): saga operation error failedExecutionContext=[{}], failures=[{}], isLastAttempt=[{}]",
            failedExecutionContext,
            failedExecutionContext.getFailures(),
            isLastAttempt,
            failedExecutionContext.getError()
        );

        var serializedException = toSerializedException(exception);
        if (!isLastAttempt) {
            stateHolder.set(serializedException);
            return false;
        }

        var currentSagaAction = sagaPipeline.getCurrentAction().toBuilder()
            .exceptionType(serializedException.exceptionType().toCanonical())
            .serializedException(serializedException.serializedException())
            .build();
        sagaPipeline.setCurrentAction(currentSagaAction);

        sagaManager.setFailResultIfExists(
            sagaPipeline.getSagaId(),
            serializedException.serializedException(),
            serializedException.exceptionType()
        );
        sagaPipeline.rewindToRevertFromCurrentPosition();
        scheduleNextOrComplete(failedExecutionContext, sagaPipeline, SagaAction::getSagaRevertMethodTaskName);

        return true;
    }

    private SerializedException toSerializedException(Throwable exception) throws IOException {
        JavaType exceptionType = TypeFactory.defaultInstance().constructType(exception.getClass());

        //reset stack trace
        exception.setStackTrace(new StackTraceElement[0]);
        byte[] serializedException = taskSerializer.writeValue(exception);

        return new SerializedException(serializedException, exceptionType);
    }

    public record SerializedException(
        byte[] serializedException,
        JavaType exceptionType
    ) {
    }
}

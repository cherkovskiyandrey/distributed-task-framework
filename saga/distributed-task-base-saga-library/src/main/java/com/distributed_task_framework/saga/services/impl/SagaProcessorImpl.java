package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.mappers.SagaTrackIdMapper;
import com.distributed_task_framework.saga.models.SagaOperation;
import com.distributed_task_framework.saga.models.SagaParsedTrackId;
import com.distributed_task_framework.saga.models.SagaPipelineContext;
import com.distributed_task_framework.saga.models.SagaTrackId;
import com.distributed_task_framework.saga.services.RevertibleBiConsumer;
import com.distributed_task_framework.saga.services.RevertibleConsumer;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.services.SagaFlowBuilder;
import com.distributed_task_framework.saga.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.saga.services.SagaFlowWithoutResult;
import com.distributed_task_framework.saga.services.SagaProcessor;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.services.SagaResultService;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.distributed_task_framework.service.DistributedTaskService;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 1. Develop full api (+)
 * 2. Support naming + versioning + exceptions for method (+)
 * 3. Pass Throwable to recovable method as @Nullable. And use it in case when exception has been thrown from current task (+)
 * 4. Ability to register unrecoverable exceptions, with lead no to retry task (+)
 * 5. AffinityGroup + affinity (+)
 * 6.1 todo: now it will not work, because DTF doesn't allow to create tasks form exception (+)
 * //          ^^^^ - fixed, need to be covered by tests (-)
 * 6.2 todo: creating tasks to use join approach is uncorrected, because it doesn't prevent to run all next pipeline of tasks in case
 * when rollback is handled (+)
 * 6. Revert pipeline (+)
 * 7. Решить вопрос с сериализацией Throwable-а (+)
 * 8. SagaProcessor::thenRun has not to receive input, because input has to be provided from root method! (+)
 * 9. SagaRegister.resolve has to be optimized and return value from cache (+)
 * - impossible, every call the same lambda code has different reference
 * 10. if method is marked as @Transactional - use EXACTLY_ONCE GUARANTIES (+)
 * 11. Ability to wait for task completion (+)
 * - wait for must be covered by tests (-)
 * 12. Create separated library + boot module + test application (+)
 * 9. Ability to wait task result (+)
 * 10. Ability to set default and custom retry settings (maybe via task settings ?) (-)
 * - in real properties from application.yaml doesn't work at all!!! because they are built in com.distributed_task_framework.autoconfigure.TaskConfigurationDiscoveryProcessor#buildTaskSettings(com.distributed_task_framework.task.Task)
 * and this logic has to be repeated in the saga library (-)
 * 8. Необходимо регистрировать 2 таски одну для прямой операции и вторую для обратной и создать регистр имя-операции:код ? (-)
 * - тут основные консерны:
 * - как быть с версионностью (-)
 * 11. Think about exactly once for remote http call (-)
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaProcessorImpl implements SagaProcessor {
    PlatformTransactionManager transactionManager;
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaResultService sagaResultService;
    SagaHelper sagaHelper;
    SagaTrackIdMapper sagaTrackIdMapper;

    @SneakyThrows
    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation,
                                                                        RevertibleBiConsumer<INPUT, OUTPUT> revertOperation,
                                                                        INPUT input) {
        Objects.requireNonNull(input);
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaOperation.getTaskDef();
        TaskDef<SagaPipelineContext> sagaRevertMethodTaskDef = sagaRegister.resolveRevert(revertOperation).getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
                SagaArguments.ROOT_INPUT,
                SagaArguments.OUTPUT,
                SagaArguments.THROWABLE
        );

        SagaPipelineContext sagaPipelineContext = sagaHelper.buildContextFor(
                null,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                sagaRevertMethodTaskDef,
                revertOperationSagaSchemaArguments,
                input
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext, sagaOperation.getMethod().getReturnType());
    }

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation, INPUT input) {
        Objects.requireNonNull(input);
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaOperation.getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);

        SagaPipelineContext sagaPipelineContext = sagaHelper.buildContextFor(
                null,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                null,
                null,
                input
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext, sagaOperation.getMethod().getReturnType());
    }

    private <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> wrapToSagaFlowBuilder(SagaPipelineContext sagaPipelineContext,
                                                                                 @Nullable Class<?> methodOutputType) {
        return SagaFlowBuilderImpl.<INPUT, OUTPUT>builder()
                .transactionManager(transactionManager)
                .sagaResultService(sagaResultService)
                .distributedTaskService(distributedTaskService)
                .sagaHelper(sagaHelper)
                .sagaRegister(sagaRegister)
                .sagaParentPipelineContext(sagaPipelineContext)
                .sagaTrackIdMapper(sagaTrackIdMapper)
                .methodOutputType(methodOutputType)
                .build();
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput<INPUT> registerToConsume(Consumer<INPUT> operation,
                                                                        RevertibleConsumer<INPUT> revertOperation,
                                                                        INPUT input) {
        Objects.requireNonNull(input);
        throw new UnsupportedOperationException();
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput<INPUT> registerToConsume(Consumer<INPUT> operation, INPUT input) {
        Objects.requireNonNull(input);
        throw new UnsupportedOperationException();
    }

    @Override
    public <OUTPUT> SagaFlow<OUTPUT> getFlow(SagaTrackId trackId, Class<OUTPUT> trackingClass) {
        SagaParsedTrackId sagaParsedTrackId = sagaTrackIdMapper.mapToParsed(trackId);
        return SagaFlowImpl.<OUTPUT>builder()
                .distributedTaskService(distributedTaskService)
                .sagaResultService(sagaResultService)
                .sagaTrackIdMapper(sagaTrackIdMapper)
                .sagaId(sagaParsedTrackId.sagaId())
                .taskId(sagaParsedTrackId.taskId())
                .resultType(trackingClass)
                .build();
    }

    @Override
    public SagaFlowWithoutResult getFlow(SagaTrackId trackId) {
        throw new UnsupportedOperationException();
    }
}

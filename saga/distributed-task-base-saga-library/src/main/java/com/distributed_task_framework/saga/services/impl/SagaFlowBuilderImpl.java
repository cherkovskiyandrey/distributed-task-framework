package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.mappers.SagaTrackIdMapper;
import com.distributed_task_framework.saga.models.SagaActionContext;
import com.distributed_task_framework.saga.models.SagaOperation;
import com.distributed_task_framework.saga.models.SagaPipelineContext;
import com.distributed_task_framework.saga.services.RevertibleBiConsumer;
import com.distributed_task_framework.saga.services.RevertibleConsumer;
import com.distributed_task_framework.saga.services.RevertibleThreeConsumer;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.services.SagaFlowBuilder;
import com.distributed_task_framework.saga.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.services.SagaResultService;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@Value
@Builder(toBuilder = true)
public class SagaFlowBuilderImpl<ROOT_INPUT, PARENT_OUTPUT> implements SagaFlowBuilder<ROOT_INPUT, PARENT_OUTPUT> {
    PlatformTransactionManager transactionManager;
    SagaResultService sagaResultService;
    DistributedTaskService distributedTaskService;
    SagaRegister sagaRegister;
    SagaHelper sagaHelper;
    TaskSerializer taskSerializer;
    SagaPipelineContext sagaParentPipelineContext;
    SagaTrackIdMapper sagaTrackIdMapper;
    @Nullable
    Class<?> methodOutputType;

    @SneakyThrows
    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(BiFunction<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> operation,
                                                                RevertibleThreeConsumer<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> revertOperation) {
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaOperation.getTaskDef();
        TaskDef<SagaPipelineContext> sagaRevertMethodTaskDef = sagaRegister.resolveRevert(revertOperation).getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(
                SagaArguments.PARENT_OUTPUT,
                SagaArguments.ROOT_INPUT
        );
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
                SagaArguments.PARENT_OUTPUT,
                SagaArguments.ROOT_INPUT,
                SagaArguments.OUTPUT,
                SagaArguments.THROWABLE
        );

        var sagaPipelineContext = sagaHelper.buildContextFor(
                sagaParentPipelineContext,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                sagaRevertMethodTaskDef,
                revertOperationSagaSchemaArguments,
                null
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext, sagaOperation.getMethod().getReturnType());
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(BiFunction<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> operation) {
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaOperation.getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(
                SagaArguments.PARENT_OUTPUT,
                SagaArguments.ROOT_INPUT
        );

        var sagaPipelineContext = sagaHelper.buildContextFor(
                sagaParentPipelineContext,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                null,
                null,
                null
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext, sagaOperation.getMethod().getReturnType());
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(Function<PARENT_OUTPUT, OUTPUT> operation,
                                                                RevertibleBiConsumer<PARENT_OUTPUT, OUTPUT> revertOperation) {
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaOperation.getTaskDef();
        TaskDef<SagaPipelineContext> revertSagaMethodTaskDef = sagaRegister.resolveRevert(revertOperation).getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.PARENT_OUTPUT);
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
                SagaArguments.PARENT_OUTPUT,
                SagaArguments.OUTPUT,
                SagaArguments.THROWABLE
        );

        var sagaPipelineContext = sagaHelper.buildContextFor(
                sagaParentPipelineContext,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                revertSagaMethodTaskDef,
                revertOperationSagaSchemaArguments,
                null
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext, sagaOperation.getMethod().getReturnType());
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(Function<PARENT_OUTPUT, OUTPUT> operation) {
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaOperation.getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.PARENT_OUTPUT);

        var sagaPipelineContext = sagaHelper.buildContextFor(
                sagaParentPipelineContext,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                null,
                null,
                null
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext, sagaOperation.getMethod().getReturnType());
    }

    @SuppressWarnings("unchecked")
    private <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> wrapToSagaFlowBuilder(SagaPipelineContext sagaPipelineContext,
                                                                               @Nullable Class<?> methodOutputType) {
        return (SagaFlowBuilder<ROOT_INPUT, OUTPUT>) this.toBuilder()
                .sagaParentPipelineContext(sagaPipelineContext)
                .methodOutputType(methodOutputType)
                .build();
    }

    @Override
    public SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(BiConsumer<PARENT_OUTPUT, ROOT_INPUT> operation,
                                                               RevertibleBiConsumer<PARENT_OUTPUT, ROOT_INPUT> revertOperation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(BiConsumer<PARENT_OUTPUT, ROOT_INPUT> operation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(Consumer<PARENT_OUTPUT> operation,
                                                               RevertibleConsumer<PARENT_OUTPUT> revertOperation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(Consumer<PARENT_OUTPUT> operation) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Transactional
    @SneakyThrows
    @Override
    public SagaFlow<PARENT_OUTPUT> startWithAffinity(String affinityGroup, String affinity) {
        UUID sagaId = sagaParentPipelineContext.getSagaId();
        sagaParentPipelineContext.rewind();
        sagaParentPipelineContext.moveToNext();
        SagaActionContext currentSagaActionContext = sagaParentPipelineContext.getCurrentSagaContext();

        TaskId taskId = distributedTaskService.schedule(
                sagaRegister.resolveByTaskName(currentSagaActionContext.getSagaMethodTaskName()),
                ExecutionContext.withAffinityGroup(
                        sagaParentPipelineContext,
                        affinityGroup,
                        affinity
                )
        );
        sagaResultService.beginWatching(sagaId);

        return SagaFlowImpl.<PARENT_OUTPUT>builder()
                .distributedTaskService(distributedTaskService)
                .sagaResultService(sagaResultService)
                .sagaId(sagaId)
                .taskId(taskId)
                .sagaTrackIdMapper(sagaTrackIdMapper)
                .resultType((Class<PARENT_OUTPUT>) methodOutputType)
                .build();
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    @Override
    public SagaFlow<PARENT_OUTPUT> start() {
        UUID sagaId = sagaParentPipelineContext.getSagaId();
        sagaParentPipelineContext.rewind();
        sagaParentPipelineContext.moveToNext();
        SagaActionContext currentSagaActionContext = sagaParentPipelineContext.getCurrentSagaContext();

        TaskId taskId = distributedTaskService.schedule(
                sagaRegister.resolveByTaskName(currentSagaActionContext.getSagaMethodTaskName()),
                ExecutionContext.simple(sagaParentPipelineContext)
        );
        sagaResultService.beginWatching(sagaId);

        return SagaFlowImpl.<PARENT_OUTPUT>builder()
                .distributedTaskService(distributedTaskService)
                .sagaResultService(sagaResultService)
                .sagaId(sagaId)
                .taskId(taskId)
                .sagaTrackIdMapper(sagaTrackIdMapper)
                .resultType((Class<PARENT_OUTPUT>) methodOutputType)
                .build();
    }
}

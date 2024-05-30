package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.saga.services.RevertibleBiConsumer;
import com.distributed_task_framework.saga.services.RevertibleConsumer;
import com.distributed_task_framework.saga.services.RevertibleThreeConsumer;
import com.distributed_task_framework.saga.services.SagaFlowBuilder;
import com.distributed_task_framework.saga.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.saga.models.SagaContext;
import com.distributed_task_framework.saga.models.SagaPipelineContext;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@Value
@Builder(toBuilder = true)
public class SagaFlowBuilderImpl<ROOT_INPUT, PARENT_OUTPUT> implements SagaFlowBuilder<ROOT_INPUT, PARENT_OUTPUT> {
    PlatformTransactionManager transactionManager;
    DistributedTaskService distributedTaskService;
    SagaRegister sagaRegister;
    SagaHelper sagaHelper;
    TaskSerializer taskSerializer;
    SagaPipelineContext sagaParentPipelineContext;

    @SneakyThrows
    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(BiFunction<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> operation,
                                                                RevertibleThreeConsumer<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> revertOperation) {
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> sagaRevertMethodTaskDef = sagaRegister.resolveRevert(revertOperation);

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

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(BiFunction<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> operation) {
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaRegister.resolve(operation);

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

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(Function<PARENT_OUTPUT, OUTPUT> operation,
                                                                RevertibleBiConsumer<PARENT_OUTPUT, OUTPUT> revertOperation) {
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> revertSagaMethodTaskDef = sagaRegister.resolveRevert(revertOperation);

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

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(Function<PARENT_OUTPUT, OUTPUT> operation) {
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaRegister.resolve(operation);

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.PARENT_OUTPUT);

        var sagaPipelineContext = sagaHelper.buildContextFor(
                sagaParentPipelineContext,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                null,
                null,
                null
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    @SuppressWarnings("unchecked")
    private <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> wrapToSagaFlowBuilder(SagaPipelineContext sagaPipelineContext) {
        return (SagaFlowBuilder<ROOT_INPUT, OUTPUT>) this.toBuilder()
                .sagaParentPipelineContext(sagaPipelineContext)
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

    @SneakyThrows
    @Override
    public SagaFlow<PARENT_OUTPUT> startWithAffinity(String affinityGroup, String affinity) {
        sagaParentPipelineContext.rewind();
        sagaParentPipelineContext.moveToNext();
        SagaContext currentSagaContext = sagaParentPipelineContext.getCurrentSagaContext();

        TaskId taskId = distributedTaskService.schedule(
                sagaRegister.resolveByTaskName(currentSagaContext.getSagaMethodTaskName()),
                ExecutionContext.withAffinityGroup(
                        sagaParentPipelineContext,
                        affinityGroup,
                        affinity
                )
        );

        return SagaFlowImpl.<PARENT_OUTPUT>builder()
                .distributedTaskService(distributedTaskService)
                .taskId(taskId)
                .build();
    }

    @SneakyThrows
    @Override
    public SagaFlow<PARENT_OUTPUT> start() {
        sagaParentPipelineContext.rewind();
        sagaParentPipelineContext.moveToNext();
        SagaContext currentSagaContext = sagaParentPipelineContext.getCurrentSagaContext();

        TaskId taskId = distributedTaskService.schedule(
                sagaRegister.resolveByTaskName(currentSagaContext.getSagaMethodTaskName()),
                ExecutionContext.simple(sagaParentPipelineContext)
        );

        return SagaFlowImpl.<PARENT_OUTPUT>builder()
                .distributedTaskService(distributedTaskService)
                .taskId(taskId)
                .build();
    }
}

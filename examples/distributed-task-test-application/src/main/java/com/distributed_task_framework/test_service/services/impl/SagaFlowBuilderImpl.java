package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.models.SagaPipelineContext;
import com.distributed_task_framework.test_service.services.BiConsumerWithThrowableArg;
import com.distributed_task_framework.test_service.services.ConsumerWithThrowableArg;
import com.distributed_task_framework.test_service.services.SagaFlow;
import com.distributed_task_framework.test_service.services.SagaFlowBuilder;
import com.distributed_task_framework.test_service.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.test_service.services.SagaRegister;
import com.distributed_task_framework.test_service.services.ThreeConsumerWithThrowableArg;
import com.distributed_task_framework.test_service.utils.SagaArguments;
import com.distributed_task_framework.test_service.utils.SagaSchemaArguments;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@Value
@Builder(toBuilder = true)
public class SagaFlowBuilderImpl<PARENT_OUTPUT> implements SagaFlowBuilder<PARENT_OUTPUT> {
    PlatformTransactionManager transactionManager;
    DistributedTaskService distributedTaskService;
    SagaRegister sagaRegister;
    SagaHelper sagaHelper;
    TaskSerializer taskSerializer;
    SagaPipelineContext sagaParentPipelineContext;

    @SneakyThrows
    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(BiFunction<PARENT_OUTPUT, INPUT, OUTPUT> operation,
                                                           ThreeConsumerWithThrowableArg<PARENT_OUTPUT, INPUT, OUTPUT> revertOperation,
                                                           INPUT input) {
        Objects.requireNonNull(input);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> sagaRevertMethodTaskDef = sagaRegister.resolveRevert(revertOperation);

        var operationSagaSchemaArguments = SagaSchemaArguments.of(
                SagaArguments.PARENT_OUTPUT,
                SagaArguments.INPUT
        );
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
                SagaArguments.PARENT_OUTPUT,
                SagaArguments.INPUT,
                SagaArguments.OUTPUT,
                SagaArguments.THROWABLE
        );

        var sagaPipelineContext = sagaHelper.buildContextFor(
                sagaParentPipelineContext,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                sagaRevertMethodTaskDef,
                revertOperationSagaSchemaArguments,
                input
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(BiFunction<PARENT_OUTPUT, INPUT, OUTPUT> operation, INPUT input) {
        Objects.requireNonNull(input);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaRegister.resolve(operation);

        var operationSagaSchemaArguments = SagaSchemaArguments.of(
                SagaArguments.PARENT_OUTPUT,
                SagaArguments.INPUT
        );

        var sagaPipelineContext = sagaHelper.buildContextFor(
                sagaParentPipelineContext,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                null,
                null,
                input
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(Function<PARENT_OUTPUT, OUTPUT> operation,
                                                    BiConsumerWithThrowableArg<PARENT_OUTPUT, OUTPUT> revertOperation) {
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
    public <OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(Function<PARENT_OUTPUT, OUTPUT> operation) {
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
    private <OUTPUT> SagaFlowBuilder<OUTPUT> wrapToSagaFlowBuilder(SagaPipelineContext sagaPipelineContext) {
        return (SagaFlowBuilder<OUTPUT>) this.toBuilder()
                .sagaParentPipelineContext(sagaPipelineContext)
                .build();
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput thenConsume(BiConsumer<PARENT_OUTPUT, INPUT> operation,
                                                           BiConsumerWithThrowableArg<PARENT_OUTPUT, INPUT> revertOperation,
                                                           INPUT input) {
        Objects.requireNonNull(input);
        throw new UnsupportedOperationException();
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput thenConsume(BiConsumer<PARENT_OUTPUT, INPUT> operation, INPUT input) {
        Objects.requireNonNull(input);
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaFlowBuilderWithoutInput thenConsume(Consumer<PARENT_OUTPUT> operation,
                                                   ConsumerWithThrowableArg<PARENT_OUTPUT> revertOperation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaFlowBuilderWithoutInput thenConsume(Consumer<PARENT_OUTPUT> operation) {
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

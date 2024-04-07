package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.models.SagaBuilderContext;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.models.SagaRevert;
import com.distributed_task_framework.test_service.models.SagaRevertInputOnly;
import com.distributed_task_framework.test_service.models.SagaRevertWithParentInput;
import com.distributed_task_framework.test_service.models.SagaRevertWithParentInputOnly;
import com.distributed_task_framework.test_service.services.SagaFlow;
import com.distributed_task_framework.test_service.services.SagaFlowBuilder;
import com.distributed_task_framework.test_service.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.test_service.services.SagaRegister;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@Value
@Builder(toBuilder = true)
public class SagaFlowBuilderImpl<PARENT_INPUT> implements SagaFlowBuilder<PARENT_INPUT> {
    PlatformTransactionManager transactionManager;
    DistributedTaskService distributedTaskService;
    SagaRegister sagaRegister;
    SagaHelper sagaHelper;
    Function<SagaBuilderContext, TaskId> prevHandler;

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(BiFunction<PARENT_INPUT, INPUT, OUTPUT> operation,
                                                           Consumer<SagaRevertWithParentInput<PARENT_INPUT, INPUT, OUTPUT>> revertOperation,
                                                           INPUT input) {
        TaskDef<SagaContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        TaskDef<SagaContext> revertSagaMethodTaskDef = sagaRegister.resolve(revertOperation);
        var handler = buildChainedSagaHandler(sagaMethodTaskDef, revertSagaMethodTaskDef, input);

        return wrapToSagaFlowBuilder(handler);
    }

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(BiFunction<PARENT_INPUT, INPUT, OUTPUT> operation, INPUT input) {
        TaskDef<SagaContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        var handler = buildChainedSagaHandler(sagaMethodTaskDef, null, input);

        return wrapToSagaFlowBuilder(handler);
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(Function<PARENT_INPUT, OUTPUT> operation,
                                                    Consumer<SagaRevert<PARENT_INPUT, OUTPUT>> revertOperation) {
        TaskDef<SagaContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        TaskDef<SagaContext> revertSagaMethodTaskDef = sagaRegister.resolve(revertOperation);
        var handler = buildChainedSagaHandler(sagaMethodTaskDef, revertSagaMethodTaskDef, null);

        return wrapToSagaFlowBuilder(handler);
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(Function<PARENT_INPUT, OUTPUT> operation) {
        TaskDef<SagaContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        var handler = buildChainedSagaHandler(sagaMethodTaskDef, null, null);

        return wrapToSagaFlowBuilder(handler);
    }

    @SuppressWarnings("Convert2Lambda")
    private <INPUT> Function<SagaBuilderContext, TaskId> buildChainedSagaHandler(TaskDef<SagaContext> operationTaskDef,
                                                                                 @Nullable TaskDef<SagaContext> revertSagaMethodTaskDef, //todo: implement
                                                                                 @Nullable INPUT input) {
        return new Function<>() {

            @SneakyThrows
            @Override
            public TaskId apply(SagaBuilderContext sagaBuilderContext) {
                var parentSagaContext = sagaBuilderContext.toBuilder()
                        .nextOperationTaskDef(operationTaskDef)
                        .build();

                TaskId parentTaskId = prevHandler.apply(parentSagaContext);
                var executionContext = sagaHelper.buildContextFor(sagaBuilderContext, input);

                return distributedTaskService.scheduleJoin(
                        operationTaskDef,
                        executionContext,
                        List.of(parentTaskId)
                );
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <OUTPUT> SagaFlowBuilder<OUTPUT> wrapToSagaFlowBuilder(Function<SagaBuilderContext, TaskId> handler) {
        return (SagaFlowBuilder<OUTPUT>) this.toBuilder()
                .prevHandler(handler)
                .build();
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput thenConsume(BiConsumer<PARENT_INPUT, INPUT> operation,
                                                           Consumer<SagaRevertWithParentInputOnly<PARENT_INPUT, INPUT>> revertOperation,
                                                           INPUT input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput thenConsume(BiConsumer<PARENT_INPUT, INPUT> operation, INPUT input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaFlowBuilderWithoutInput thenConsume(Consumer<PARENT_INPUT> operation, Consumer<SagaRevertInputOnly<PARENT_INPUT>> revertOperation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaFlowBuilderWithoutInput thenConsume(Consumer<PARENT_INPUT> operation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaFlow<PARENT_INPUT> startWithAffinity(String affinityGroup, String affinity) {
        var sagaContext = SagaBuilderContext.EMPTY_CONTEXT.toBuilder()
                .affinityGroup(affinityGroup)
                .affinity(affinity)
                .build();

        TaskId lastTaskId = new TransactionTemplate(transactionManager)
                .execute(status -> prevHandler.apply(sagaContext));

        return SagaFlowImpl.<PARENT_INPUT>builder()
                .distributedTaskService(distributedTaskService)
                .taskId(lastTaskId)
                .build();
    }

    @Override
    public SagaFlow<PARENT_INPUT> start() {
        TaskId lastTaskId = new TransactionTemplate(transactionManager)
                .execute(status -> prevHandler.apply(SagaBuilderContext.EMPTY_CONTEXT));

        return SagaFlowImpl.<PARENT_INPUT>builder()
                .distributedTaskService(distributedTaskService)
                .taskId(lastTaskId)
                .build();
    }
}

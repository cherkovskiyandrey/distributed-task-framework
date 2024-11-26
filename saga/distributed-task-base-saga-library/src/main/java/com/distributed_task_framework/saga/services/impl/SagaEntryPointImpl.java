package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaEmbeddedPipelineContext;
import com.distributed_task_framework.saga.models.SagaOperation;
import com.distributed_task_framework.saga.services.RevertibleBiConsumer;
import com.distributed_task_framework.saga.services.RevertibleConsumer;
import com.distributed_task_framework.saga.services.SagaContextService;
import com.distributed_task_framework.saga.services.SagaEntryPoint;
import com.distributed_task_framework.saga.services.SagaFlowBuilder;
import com.distributed_task_framework.saga.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.distributed_task_framework.service.DistributedTaskService;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@Value
@Builder(toBuilder = true)
public class SagaEntryPointImpl implements SagaEntryPoint {
    String userName;
    @Nullable
    String affinityGroup;
    @Nullable
    String affinity;
    PlatformTransactionManager transactionManager;
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaContextService sagaContextService;
    SagaHelper sagaHelper;

    @SneakyThrows
    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation,
                                                                        RevertibleBiConsumer<INPUT, OUTPUT> revertOperation,
                                                                        INPUT input) {
        Objects.requireNonNull(input);
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaEmbeddedPipelineContext> sagaMethodTaskDef = sagaOperation.getTaskDef();
        TaskDef<SagaEmbeddedPipelineContext> sagaRevertMethodTaskDef = sagaRegister.resolveRevert(revertOperation).getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
            SagaArguments.ROOT_INPUT,
            SagaArguments.OUTPUT,
            SagaArguments.THROWABLE
        );

        SagaEmbeddedPipelineContext sagaEmbeddedPipelineContext = sagaHelper.buildContextFor(
            null,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            sagaRevertMethodTaskDef,
            revertOperationSagaSchemaArguments,
            input
        );

        return wrapToSagaFlowBuilder(sagaEmbeddedPipelineContext, sagaOperation.getMethod().getReturnType());
    }

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation, INPUT input) {
        Objects.requireNonNull(input);
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaEmbeddedPipelineContext> sagaMethodTaskDef = sagaOperation.getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);

        SagaEmbeddedPipelineContext sagaEmbeddedPipelineContext = sagaHelper.buildContextFor(
            null,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            null,
            null,
            input
        );

        return wrapToSagaFlowBuilder(sagaEmbeddedPipelineContext, sagaOperation.getMethod().getReturnType());
    }

    private <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> wrapToSagaFlowBuilder(SagaEmbeddedPipelineContext sagaEmbeddedPipelineContext,
                                                                                 @Nullable Class<?> methodOutputType) {
        return SagaFlowBuilderImpl.<INPUT, OUTPUT>builder()
            .userName(userName)
            .affinityGroup(affinityGroup)
            .affinity(affinity)
            .transactionManager(transactionManager)
            .sagaContextService(sagaContextService)
            .distributedTaskService(distributedTaskService)
            .sagaHelper(sagaHelper)
            .sagaRegister(sagaRegister)
            .sagaParentEmbeddedPipelineContext(sagaEmbeddedPipelineContext)
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
}

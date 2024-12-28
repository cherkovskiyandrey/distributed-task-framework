package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.models.SagaOperation;
import com.distributed_task_framework.saga.services.SagaFunction;
import com.distributed_task_framework.saga.services.SagaRevertibleBiConsumer;
import com.distributed_task_framework.saga.services.RevertibleConsumer;
import com.distributed_task_framework.saga.services.SagaFlowEntryPoint;
import com.distributed_task_framework.saga.services.SagaFlowBuilder;
import com.distributed_task_framework.saga.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaRegister;
import com.distributed_task_framework.saga.utils.LambdaHelper;
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
public class SagaFlowEntryPointImpl implements SagaFlowEntryPoint {
    String name;
    @Nullable
    String affinityGroup;
    @Nullable
    String affinity;
    PlatformTransactionManager transactionManager;
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaManager sagaManager;
    SagaHelper sagaHelper;

    @SneakyThrows
    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(SagaFunction<INPUT, OUTPUT> operation,
                                                                        SagaRevertibleBiConsumer<INPUT, OUTPUT> revertOperation,
                                                                        INPUT input) {
        Objects.requireNonNull(input);
        LambdaHelper.printName(operation);
        LambdaHelper.printName(revertOperation);
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperation.getTaskDef();
        TaskDef<SagaPipeline> sagaRevertMethodTaskDef = sagaRegister.resolveRevert(revertOperation).getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
            SagaArguments.ROOT_INPUT,
            SagaArguments.OUTPUT,
            SagaArguments.THROWABLE
        );

        SagaPipeline sagaPipeline = sagaHelper.buildContextFor(
            null,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            sagaRevertMethodTaskDef,
            revertOperationSagaSchemaArguments,
            input
        );

        return wrapToSagaFlowBuilder(sagaPipeline, sagaOperation.getMethod().getReturnType());
    }

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation, INPUT input) {
        Objects.requireNonNull(input);
        SagaOperation sagaOperation = sagaRegister.resolve(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperation.getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);

        SagaPipeline sagaPipeline = sagaHelper.buildContextFor(
            null,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            null,
            null,
            input
        );

        return wrapToSagaFlowBuilder(sagaPipeline, sagaOperation.getMethod().getReturnType());
    }

    private <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> wrapToSagaFlowBuilder(SagaPipeline sagaPipeline,
                                                                                 @Nullable Class<?> methodOutputType) {
        return SagaFlowBuilderImpl.<INPUT, OUTPUT>builder()
            .name(name)
            .affinityGroup(affinityGroup)
            .affinity(affinity)
            .transactionManager(transactionManager)
            .sagaManager(sagaManager)
            .distributedTaskService(distributedTaskService)
            .sagaHelper(sagaHelper)
            .sagaRegister(sagaRegister)
            .sagaParentPipeline(sagaPipeline)
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

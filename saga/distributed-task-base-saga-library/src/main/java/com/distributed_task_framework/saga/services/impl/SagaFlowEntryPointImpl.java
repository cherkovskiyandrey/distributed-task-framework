package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.functions.SagaConsumer;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.functions.SagaRevertibleBiConsumer;
import com.distributed_task_framework.saga.functions.SagaRevertibleConsumer;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.SagaFlowBuilder;
import com.distributed_task_framework.saga.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.saga.services.SagaFlowEntryPoint;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.settings.SagaSettings;
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

@Slf4j
@Value
@Builder(toBuilder = true)
public class SagaFlowEntryPointImpl implements SagaFlowEntryPoint {
    String name;
    SagaSettings sagaSettings;
    @Nullable
    String affinityGroup;
    @Nullable
    String affinity;
    PlatformTransactionManager transactionManager;
    SagaResolver sagaResolver;
    DistributedTaskService distributedTaskService;
    SagaManager sagaManager;
    SagaHelper sagaHelper;

    @SneakyThrows
    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(SagaFunction<INPUT, OUTPUT> operation,
                                                                        SagaRevertibleBiConsumer<INPUT, OUTPUT> revertOperation,
                                                                        INPUT input) {
        Objects.requireNonNull(input);
        SagaOperand sagaOperand = sagaResolver.resolveAsOperand(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperand.getTaskDef();
        TaskDef<SagaPipeline> sagaRevertMethodTaskDef = sagaResolver.resolveAsOperand(revertOperation).getTaskDef();

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

        return wrapToSagaFlowBuilder(sagaPipeline, sagaOperand.getMethod().getReturnType());
    }

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(SagaFunction<INPUT, OUTPUT> operation,
                                                                        INPUT input) {
        Objects.requireNonNull(input);
        SagaOperand sagaOperand = sagaResolver.resolveAsOperand(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperand.getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);

        SagaPipeline sagaPipeline = sagaHelper.buildContextFor(
            null,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            null,
            null,
            input
        );

        return wrapToSagaFlowBuilder(sagaPipeline, sagaOperand.getMethod().getReturnType());
    }

    private <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> wrapToSagaFlowBuilder(SagaPipeline sagaPipeline,
                                                                                 @Nullable Class<?> methodOutputType) {
        return SagaFlowBuilderImpl.<INPUT, OUTPUT>builder()
            .name(name)
            .sagaSettings(sagaSettings)
            .affinityGroup(affinityGroup)
            .affinity(affinity)
            .transactionManager(transactionManager)
            .sagaManager(sagaManager)
            .distributedTaskService(distributedTaskService)
            .sagaResolver(sagaResolver)
            .sagaHelper(sagaHelper)
            .sagaParentPipeline(sagaPipeline)
            .methodOutputType(methodOutputType)
            .build();
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput<INPUT> registerToConsume(SagaConsumer<INPUT> operation,
                                                                        SagaRevertibleConsumer<INPUT> revertOperation,
                                                                        INPUT input) {
        SagaOperand sagaOperand = sagaResolver.resolveAsOperand(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperand.getTaskDef();
        TaskDef<SagaPipeline> sagaRevertMethodTaskDef = sagaResolver.resolveAsOperand(revertOperation).getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
            SagaArguments.ROOT_INPUT,
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

        return wrapToSagaFlowBuilder(sagaPipeline);
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput<INPUT> registerToConsume(SagaConsumer<INPUT> operation, INPUT input) {
        Objects.requireNonNull(input);
        SagaOperand sagaOperand = sagaResolver.resolveAsOperand(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperand.getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);

        SagaPipeline sagaPipeline = sagaHelper.buildContextFor(
            null,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            null,
            null,
            input
        );
        return wrapToSagaFlowBuilder(sagaPipeline);
    }

    private <INPUT> SagaFlowBuilderWithoutInput<INPUT> wrapToSagaFlowBuilder(SagaPipeline sagaPipeline) {
        return SagaFlowBuilderWithoutInputImpl.<INPUT>builder()
            .name(name)
            .sagaSettings(sagaSettings)
            .affinityGroup(affinityGroup)
            .affinity(affinity)
            .transactionManager(transactionManager)
            .sagaManager(sagaManager)
            .distributedTaskService(distributedTaskService)
            .sagaResolver(sagaResolver)
            .sagaHelper(sagaHelper)
            .sagaParentPipeline(sagaPipeline)
            .build();
    }
}

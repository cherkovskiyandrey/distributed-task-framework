package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.exceptions.SagaNotStartedException;
import com.distributed_task_framework.saga.functions.SagaConsumer;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.functions.SagaRevertibleBiConsumer;
import com.distributed_task_framework.saga.functions.SagaRevertibleConsumer;
import com.distributed_task_framework.saga.models.CreateSagaRequest;
import com.distributed_task_framework.saga.models.SagaAction;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.SagaFlowBuilder;
import com.distributed_task_framework.saga.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.saga.services.SagaFlowWithoutResult;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.saga.utils.SagaArguments;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.distributed_task_framework.service.DistributedTaskService;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

@Slf4j
@Value
@Builder(toBuilder = true)
public class SagaFlowBuilderWithoutInputImpl<ROOT_INPUT> implements SagaFlowBuilderWithoutInput<ROOT_INPUT> {
    String name;
    SagaSettings sagaSettings;
    @Nullable
    String affinityGroup;
    @Nullable
    String affinity;
    PlatformTransactionManager transactionManager;
    SagaManager sagaManager;
    DistributedTaskService distributedTaskService;
    SagaResolver sagaResolver;
    SagaHelper sagaHelper;
    SagaPipeline sagaParentPipeline;

    @Override
    public SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(SagaConsumer<ROOT_INPUT> operation,
                                                               SagaRevertibleConsumer<ROOT_INPUT> revertOperation) {
        SagaOperand sagaOperand = sagaResolver.resolveAsOperand(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperand.getTaskDef();
        TaskDef<SagaPipeline> sagaRevertMethodTaskDef = sagaResolver.resolveAsOperand(revertOperation).getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
            SagaArguments.ROOT_INPUT,
            SagaArguments.THROWABLE
        );

        var sagaPipelineContext = sagaHelper.buildContextFor(
            sagaParentPipeline,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            sagaRevertMethodTaskDef,
            revertOperationSagaSchemaArguments,
            null
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    @Override
    public SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(SagaConsumer<ROOT_INPUT> operation) {
        SagaOperand sagaOperand = sagaResolver.resolveAsOperand(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperand.getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);
        var sagaPipelineContext = sagaHelper.buildContextFor(
            sagaParentPipeline,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            null,
            null,
            null
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    private SagaFlowBuilderWithoutInput<ROOT_INPUT> wrapToSagaFlowBuilder(SagaPipeline sagaPipeline) {
        return this.toBuilder()
            .name(name)
            .affinityGroup(affinityGroup)
            .affinity(affinity)
            .sagaParentPipeline(sagaPipeline)
            .build();
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(SagaFunction<ROOT_INPUT, OUTPUT> operation,
                                                                SagaRevertibleBiConsumer<ROOT_INPUT, OUTPUT> revertOperation) {
        SagaOperand sagaOperand = sagaResolver.resolveAsOperand(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperand.getTaskDef();
        TaskDef<SagaPipeline> revertSagaMethodTaskDef = sagaResolver.resolveAsOperand(revertOperation).getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
            SagaArguments.ROOT_INPUT,
            SagaArguments.OUTPUT,
            SagaArguments.THROWABLE
        );

        var sagaPipelineContext = sagaHelper.buildContextFor(
            sagaParentPipeline,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            revertSagaMethodTaskDef,
            revertOperationSagaSchemaArguments,
            null
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext, sagaOperand.getMethod().getReturnType());
    }

    @Override
    public <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(SagaFunction<ROOT_INPUT, OUTPUT> operation) {
        SagaOperand sagaOperand = sagaResolver.resolveAsOperand(operation);
        TaskDef<SagaPipeline> sagaMethodTaskDef = sagaOperand.getTaskDef();

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.ROOT_INPUT);
        var sagaPipelineContext = sagaHelper.buildContextFor(
            sagaParentPipeline,
            sagaMethodTaskDef,
            operationSagaSchemaArguments,
            null,
            null,
            null
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext, sagaOperand.getMethod().getReturnType());
    }

    private <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> wrapToSagaFlowBuilder(SagaPipeline sagaPipeline,
                                                                               @Nullable Class<?> methodOutputType) {
        return SagaFlowBuilderImpl.<ROOT_INPUT, OUTPUT>builder()
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
    public SagaFlowWithoutResult start() {
        UUID sagaId = sagaParentPipeline.getSagaId();
        sagaParentPipeline.rewind();
        sagaParentPipeline.moveToNext();
        SagaAction currentSagaAction = sagaParentPipeline.getCurrentAction();

        try {
            new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                    TaskId taskId;
                    try {
                        taskId = distributedTaskService.schedule(
                            sagaResolver.resolveByTaskName(currentSagaAction.getSagaMethodTaskName()),
                            makeContext(sagaParentPipeline)
                        );
                    } catch (Exception e) {
                        throw new SagaNotStartedException(e);
                    }

                    log.info("start(): sagaId=[{}], sagaParentPipeline=[{}]", sagaId, sagaParentPipeline);
                    var sagaContext = CreateSagaRequest.builder()
                        .sagaId(sagaId)
                        .name(name)
                        .rootTaskId(taskId)
                        .sagaPipeline(sagaParentPipeline)
                        .build();
                    sagaManager.create(sagaContext, sagaSettings);
                }
            );
        } catch (SagaNotStartedException e) {
            throw e;
        } catch (Exception e) {
            throw new SagaNotStartedException(e);
        }

        return SagaFlowWithoutResultImpl.builderWithoutResult()
            .distributedTaskService(distributedTaskService)
            .sagaManager(sagaManager)
            .sagaId(sagaId)
            .build();
    }

    private ExecutionContext<SagaPipeline> makeContext(SagaPipeline sagaPipeline) {
        return StringUtils.isNotBlank(affinityGroup) && StringUtils.isNotBlank(affinity) ?
            ExecutionContext.withAffinityGroup(
                sagaPipeline,
                affinityGroup,
                affinity
            ) :
            ExecutionContext.simple(sagaPipeline);
    }
}

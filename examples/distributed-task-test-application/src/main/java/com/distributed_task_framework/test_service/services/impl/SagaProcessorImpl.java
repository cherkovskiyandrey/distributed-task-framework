package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.models.SagaPipelineContext;
import com.distributed_task_framework.test_service.models.SagaTrackId;
import com.distributed_task_framework.test_service.services.BiConsumerWithThrowableArg;
import com.distributed_task_framework.test_service.services.ConsumerWithThrowableArg;
import com.distributed_task_framework.test_service.services.SagaFlow;
import com.distributed_task_framework.test_service.services.SagaFlowBuilder;
import com.distributed_task_framework.test_service.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.test_service.services.SagaFlowWithoutResult;
import com.distributed_task_framework.test_service.services.SagaProcessor;
import com.distributed_task_framework.test_service.services.SagaRegister;
import com.distributed_task_framework.test_service.utils.SagaArguments;
import com.distributed_task_framework.test_service.utils.SagaSchemaArguments;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 1. Develop full api (+)
 * 2. Support naming + versioning + exceptions for method (+)
 * 3. Pass Throwable to recovable method as @Nullable. And use it in case when exception has been thrown from current task (+)
 * 4. Ability to register unrecoverable exceptions, with lead no to retry task (+)
 * 5. AffinityGroup + affinity (+)
 * 6. Revert pipeline (-)
 * 6.1 todo: now it will not work, because DTF doesn't allow to create tasks form exception (+)
 * //          ^^^^ - fixed, need to be covered by tests (-)
 * 6.2 todo: creating tasks to use join approach is uncorrected, because it doesn't prevent to run all next pipeline of tasks in case
 * when rollback is handled (+)
 * 7. if method is marked as @Transactional - use EXACTLY_ONCE GUARANTIES (-)
 * 8. Ability to set default and custom retry settings (maybe via task settings ?) (-)
 *      - in real properties from application.yaml doesn't work at all!!! because they are built in com.distributed_task_framework.autoconfigure.TaskConfigurationDiscoveryProcessor#buildTaskSettings(com.distributed_task_framework.task.Task)
 *      and this logic has to be repeated in the saga library (-)
 * 9. Ability to wait for task completion (-)
 * 10. Ability to wait task result (-)
 * 11. Think about exactly once for remote http call (-)
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaProcessorImpl implements SagaProcessor {
    PlatformTransactionManager transactionManager;
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaHelper sagaHelper;

    @SneakyThrows
    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation,
                                                                 BiConsumerWithThrowableArg<INPUT, OUTPUT> revertOperation,
                                                                 INPUT input) {
        Objects.requireNonNull(input);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        TaskDef<SagaPipelineContext> sagaRevertMethodTaskDef = sagaRegister.resolveRevert(revertOperation);

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.INPUT);
        var revertOperationSagaSchemaArguments = SagaSchemaArguments.of(
                SagaArguments.INPUT,
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

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation, INPUT input) {
        Objects.requireNonNull(input);
        TaskDef<SagaPipelineContext> sagaMethodTaskDef = sagaRegister.resolve(operation);

        var operationSagaSchemaArguments = SagaSchemaArguments.of(SagaArguments.INPUT);

        SagaPipelineContext sagaPipelineContext = sagaHelper.buildContextFor(
                null,
                sagaMethodTaskDef,
                operationSagaSchemaArguments,
                null,
                null,
                input
        );

        return wrapToSagaFlowBuilder(sagaPipelineContext);
    }

    private <OUTPUT> SagaFlowBuilder<OUTPUT> wrapToSagaFlowBuilder(SagaPipelineContext sagaPipelineContext) {
        return SagaFlowBuilderImpl.<OUTPUT>builder()
                .transactionManager(transactionManager)
                .distributedTaskService(distributedTaskService)
                .sagaHelper(sagaHelper)
                .sagaRegister(sagaRegister)
                .sagaParentPipelineContext(sagaPipelineContext)
                .build();
    }

    //todo: looks we don't pass TaskDef for next task, because we don't provide any result,
    //but in order to implement revert procedure we must to pass to next task revert TaskDef + Input
    @Override
    public <INPUT> SagaFlowBuilderWithoutInput registerToConsume(Consumer<INPUT> operation,
                                                                 ConsumerWithThrowableArg<INPUT> revertOperation,
                                                                 INPUT input) {
        Objects.requireNonNull(input);
        throw new UnsupportedOperationException();
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput registerToConsume(Consumer<INPUT> operation, INPUT input) {
        Objects.requireNonNull(input);
        throw new UnsupportedOperationException();
    }

    @Override
    public <OUTPUT> Optional<SagaFlow<OUTPUT>> getFlow(SagaTrackId trackId, Class<OUTPUT> trackingClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaFlowWithoutResult getFlow(SagaTrackId trackId) {
        throw new UnsupportedOperationException();
    }
}

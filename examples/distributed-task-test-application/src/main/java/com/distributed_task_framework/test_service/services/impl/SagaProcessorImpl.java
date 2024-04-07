package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.models.SagaBuilderContext;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.models.SagaRevert;
import com.distributed_task_framework.test_service.models.SagaRevertInputOnly;
import com.distributed_task_framework.test_service.models.SagaTrackId;
import com.distributed_task_framework.test_service.services.SagaFlow;
import com.distributed_task_framework.test_service.services.SagaFlowBuilder;
import com.distributed_task_framework.test_service.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.test_service.services.SagaFlowWithoutResult;
import com.distributed_task_framework.test_service.services.SagaProcessor;
import com.distributed_task_framework.test_service.services.SagaRegister;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.Nullable;
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
 * 7. if method is marked as @Transactional - use EXACTLY_ONCE GUARANTIES (-)
 * 8. Ability to set default and custom retry settings (maybe via task settings ?) (-)
 * 9. Ability to wait for task completion (-)
 * 10. Ability to wait task result (-)
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaProcessorImpl implements SagaProcessor {
    PlatformTransactionManager transactionManager;
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaHelper sagaHelper;

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation,
                                                                 Consumer<SagaRevert<INPUT, OUTPUT>> revertOperation,
                                                                 INPUT input) {
        TaskDef<SagaContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        TaskDef<SagaContext> revertSagaMethodTaskDef = sagaRegister.resolve(revertOperation);
        var startHandler = buildStartHandler(sagaMethodTaskDef, revertSagaMethodTaskDef, input);

        return wrapToSagaFlowBuilder(startHandler);
    }

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation, INPUT input) {
        TaskDef<SagaContext> sagaMethodTaskDef = sagaRegister.resolve(operation);
        var startHandler = buildStartHandler(sagaMethodTaskDef, null, input);

        return wrapToSagaFlowBuilder(startHandler);
    }

    @SuppressWarnings("Convert2Lambda")
    private Function<SagaBuilderContext, TaskId> buildStartHandler(TaskDef<SagaContext> sagaMethodTaskDef,
                                                                   @Nullable TaskDef<SagaContext> revertSagaMethodRef, //todo: revert operations!
                                                                   @Nullable Object object) {
        return new Function<>() {

            @SneakyThrows
            @Override
            public TaskId apply(SagaBuilderContext sagaBuilderContext) {
                var executionContext = sagaHelper.buildContextFor(sagaBuilderContext, object);

                return distributedTaskService.schedule(
                        sagaMethodTaskDef,
                        executionContext
                );
            }
        };
    }

    private <OUTPUT> SagaFlowBuilder<OUTPUT> wrapToSagaFlowBuilder(Function<SagaBuilderContext, TaskId> startHandler) {
        return SagaFlowBuilderImpl.<OUTPUT>builder()
                .transactionManager(transactionManager)
                .distributedTaskService(distributedTaskService)
                .sagaHelper(sagaHelper)
                .sagaRegister(sagaRegister)
                .prevHandler(startHandler)
                .build();
    }

    //todo: looks we don't pass TaskDef for next task, because we don't provide any result,
    //but in order to implement revert procedure we mast to pass to next task revert TaskDef + Input
    @Override
    public <INPUT> SagaFlowBuilderWithoutInput registerToConsume(Consumer<INPUT> operation,
                                                                 Consumer<SagaRevertInputOnly<INPUT>> revertOperation,
                                                                 INPUT input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <INPUT> SagaFlowBuilderWithoutInput registerToConsume(Consumer<INPUT> operation, INPUT input) {
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

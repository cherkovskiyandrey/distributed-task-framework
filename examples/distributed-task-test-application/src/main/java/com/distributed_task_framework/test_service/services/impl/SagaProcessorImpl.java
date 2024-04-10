package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.models.SagaBuilderContext;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.models.SagaRevert;
import com.distributed_task_framework.test_service.models.SagaRevertContext;
import com.distributed_task_framework.test_service.models.SagaRevertInputOnly;
import com.distributed_task_framework.test_service.models.SagaTrackId;
import com.distributed_task_framework.test_service.services.SagaFlow;
import com.distributed_task_framework.test_service.services.SagaFlowBuilder;
import com.distributed_task_framework.test_service.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.test_service.services.SagaFlowWithoutResult;
import com.distributed_task_framework.test_service.services.SagaProcessor;
import com.distributed_task_framework.test_service.services.SagaRegister;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;

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
 * 6.1 todo: now it will not work, because DTF doesn't allow to create tasks form exception
 *     //          ^^^^ - fixed, need to be covered by tests (-)
 * 6.2 todo: creating tasks to use join approach is uncorrected, because it doesn't prevent to run all next pipeline of tasks in case
 *      when rollback is handled (-)
 * 7. if method is marked as @Transactional - use EXACTLY_ONCE GUARANTIES (-)
 * 8. Ability to set default and custom retry settings (maybe via task settings ?) (-)
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
                                                                 Consumer<SagaRevert<INPUT, OUTPUT>> revertOperation,
                                                                 INPUT input) {
        TaskDef<SagaContext> sagaMethodTaskDef = sagaRegister.resolve(operation, SagaContext.class);
        TaskDef<SagaRevertContext> sagaRevertMethodTaskDef = sagaRegister.resolve(revertOperation, SagaRevertContext.class);
        var startHandler = buildStartHandler(sagaMethodTaskDef, sagaRevertMethodTaskDef, input);

        return wrapToSagaFlowBuilder(startHandler);
    }

    @Override
    public <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> registerToRun(Function<INPUT, OUTPUT> operation, INPUT input) {
        TaskDef<SagaContext> sagaMethodTaskDef = sagaRegister.resolve(operation, SagaContext.class);
        var startHandler = buildStartHandler(sagaMethodTaskDef, null, input);

        return wrapToSagaFlowBuilder(startHandler);
    }

    @SuppressWarnings("Convert2Lambda")
    private Function<SagaBuilderContext, TaskId> buildStartHandler(TaskDef<SagaContext> sagaMethodTaskDef,
                                                                   @Nullable TaskDef<SagaRevertContext> sagaRevertMethodRef,
                                                                   @Nullable Object object) {
        return new Function<>() {

            @SneakyThrows
            @Override
            public TaskId apply(SagaBuilderContext sagaBuilderContext) {
                var executionContext = sagaHelper.buildContextFor(
                        sagaBuilderContext,
                        sagaRevertMethodRef,
                        object
                );

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

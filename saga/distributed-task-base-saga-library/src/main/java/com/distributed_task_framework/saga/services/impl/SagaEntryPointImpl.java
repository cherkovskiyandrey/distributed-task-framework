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

/**
 * 1. Develop full api (+)
 * 2. Support naming + versioning + exceptions for method (+)
 * 3. Pass Throwable to recovable method as @Nullable. And use it in case when exception has been thrown from current task (+)
 * 4. Ability to register unrecoverable exceptions, with lead no to retry task (+)
 * 5. AffinityGroup + affinity (+)
 * 6.1 todo: now it will not work, because DTF doesn't allow to create tasks form exception (+)
 * //          ^^^^ - fixed, need to be covered by tests (-)
 * 6.2 todo: creating tasks to use join approach is uncorrected, because it doesn't prevent to run all next pipeline of tasks in case
 * when rollback is handled (+)
 * 6. Revert pipeline (+)
 * 7. Решить вопрос с сериализацией Throwable-а (+)
 * 8. SagaProcessor::thenRun has not to receive input, because input has to be provided from root method! (+)
 * 9. SagaRegister.resolve has to be optimized and return value from cache (+)
 * - impossible, every call the same lambda code has different reference
 * 10. if method is marked as @Transactional - use EXACTLY_ONCE GUARANTIES (+)
 * 11. Ability to wait for task completion (+)
 * - wait for must be covered by tests (-)
 * 12. Create separated library + boot module + test application (+)
 * 9. Ability to wait task result (+)
 * 10. Проверить:
 *  - SagaContextServiceImpl#track + SagaContextServiceImpl#handleExpiredSagas - на последнем этапе уйти в таймат + заодно как отработает cancel (-)
 *  - isCompleted - дергаем ручку (-)
 * 10. Ability to set default and custom retry settings (maybe via task settings ?) (+)
 * - in real properties from application.yaml doesn't work at all!!! because they are built in com.distributed_task_framework.autoconfigure.TaskConfigurationDiscoveryProcessor#buildTaskSettings(com.distributed_task_framework.task.Task)
 * and this logic has to be repeated in the saga library or separated and moved to dedicated module (+)
 * 11. Покрыть абсолютно все тестами (-)
 * 12. Написать более честный example используя мок сервис для эмуляции взаимодействия с внешним сервисом (-)
 * 13. Подумать над перф тестом (-)
 * 14. Времеено ронять контескт если кол-во тасок больше чем dtf может за раз запланировать (-) ???
 * 9. Пожержка map/reduce через расширение контекста SagaEmbeddedPipelineContext и динамического достраивания DAG-а из самих тасок
 *  (вернее во время старта - достаточно создать 1 уровнеь map/reduce, дальше из тасок динамически достраивать DAG) (-)
 * 11. Think about exactly once for remote http call - could be possible only based on remote tasks (-)
 */
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

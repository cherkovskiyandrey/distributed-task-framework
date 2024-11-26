package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.services.SagaContextService;
import com.distributed_task_framework.saga.services.SagaEntryPoint;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.services.SagaFlowWithoutResult;
import com.distributed_task_framework.saga.services.SagaProcessor;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.utils.StringUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.UUID;



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
 * 10. Ability to set default and custom retry settings (maybe via task settings ?) (+)
 * - in real properties from application.yaml doesn't work at all!!! because they are built in com.distributed_task_framework.autoconfigure.TaskConfigurationDiscoveryProcessor#buildTaskSettings(com.distributed_task_framework.task.Task)
 * and this logic has to be repeated in the saga library or separated and moved to dedicated module (+)
 * 10. Проверить:
 *  - SagaContextServiceImpl#track + SagaContextServiceImpl#handleExpiredSagas -
 *      - на последнем этапе уйти в таймат + заодно как отработает cancel (-)
 *  - isCompleted - дергаем ручку (-)
 * 11. Реализовать возможность отмены саги - который будет останавливать сагу на текущем моменте
 *  и запускать ролбэк асинхронный (-)
 * 11. Покрыть абсолютно все тестами (-)
 * 12. Написать более честный example используя мок сервис для эмуляции взаимодействия с внешним сервисом (-)
 * 13. Подумать над перф тестом (-)
 * 14. Времеено ронять контескт если кол-во тасок больше чем dtf может за раз запланировать (-) ???
 * 9. Пожержка map/reduce через расширение контекста SagaEmbeddedPipelineContext и динамического достраивания DAG-а из самих тасок
 *  (вернее во время старта - достаточно создать 1 уровнеь map/reduce, дальше из тасок динамически достраивать DAG) (-)
 * 11. Think about exactly once for remote http call - could be possible only based on remote tasks (-)
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaProcessorImpl implements SagaProcessor {
    PlatformTransactionManager transactionManager;
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaContextService sagaContextService;
    SagaHelper sagaHelper;

    @Override
    public SagaEntryPoint create(String name) {
        StringUtils.requireNotBlank(name, "name");
        return SagaEntryPointImpl.builder()
                .userName(name)
                .transactionManager(transactionManager)
                .sagaRegister(sagaRegister)
                .distributedTaskService(distributedTaskService)
                .sagaContextService(sagaContextService)
                .sagaHelper(sagaHelper)
                .build();
    }

    @Override
    public SagaEntryPoint createWithAffinity(String name, String affinityGroup, String affinity) {
        StringUtils.requireNotBlank(name, "name");
        StringUtils.requireNotBlank(affinityGroup, "affinityGroup");
        StringUtils.requireNotBlank(affinity, "affinity");
        return SagaEntryPointImpl.builder()
                .userName(name)
                .affinityGroup(affinityGroup)
                .affinity(affinity)
                .transactionManager(transactionManager)
                .sagaRegister(sagaRegister)
                .distributedTaskService(distributedTaskService)
                .sagaContextService(sagaContextService)
                .sagaHelper(sagaHelper)
                .build();
    }

    @Override
    public <OUTPUT> SagaFlow<OUTPUT> getFlow(UUID trackId, Class<OUTPUT> trackingClass) {
        return SagaFlowImpl.<OUTPUT>builder()
                .distributedTaskService(distributedTaskService)
                .sagaContextService(sagaContextService)
                .sagaId(trackId)
                .resultType(trackingClass)
                .build();
    }

    @Override
    public SagaFlowWithoutResult getFlow(UUID trackId) {
        //todo
        throw new UnsupportedOperationException();
    }
}

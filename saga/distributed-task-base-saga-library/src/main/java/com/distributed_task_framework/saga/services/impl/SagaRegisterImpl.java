package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.annotations.SagaMethod;
import com.distributed_task_framework.saga.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.exceptions.SagaMethodDuplicateException;
import com.distributed_task_framework.saga.exceptions.SagaMethodResolvingException;
import com.distributed_task_framework.saga.exceptions.SagaTaskNotFoundException;
import com.distributed_task_framework.saga.models.SagaOperation;
import com.distributed_task_framework.saga.models.SagaPipelineContext;
import com.distributed_task_framework.saga.services.RevertibleBiConsumer;
import com.distributed_task_framework.saga.services.RevertibleThreeConsumer;
import com.distributed_task_framework.saga.services.SagaContextDiscovery;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.services.SagaTaskFactory;
import com.distributed_task_framework.saga.utils.ReflectionHelper;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.settings.TaskSettings;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ReflectionUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaRegisterImpl implements SagaRegister, BeanPostProcessor {
    private static final String TASK_PREFIX = "_____SAGA";
    private static final String TASK_REVERT_PREFIX = "_____SAGA_REVERT";
    private static final String TASK_NAME_DELIMITER = "_";

    Map<String, SagaOperation> methodToSagaOperation = Maps.newHashMap();
    Map<String, SagaOperation> revertMethodToSagaOperation = Maps.newHashMap();
    DistributedTaskService distributedTaskService;
    TaskRegistryService taskRegistryService;
    SagaContextDiscovery sagaContextDiscovery;
    SagaTaskFactory sagaTaskFactory;

    @Override
    public <IN, OUT> SagaOperation resolve(Function<IN, OUT> operation) {
        SagaMethod sagaMethod = sagaMethodByRunnable(() -> operation.apply(null), SagaMethod.class);
        return resolveByMethodRef(sagaMethod);
    }

    @Override
    public <IN> SagaOperation resolve(Consumer<IN> operation) {
        SagaMethod sagaMethod = sagaMethodByRunnable(() -> operation.accept(null), SagaMethod.class);
        return resolveByMethodRef(sagaMethod);
    }

    @Override
    public <T, U, R> SagaOperation resolve(BiFunction<T, U, R> operation) {
        SagaMethod sagaMethod = sagaMethodByRunnable(() -> operation.apply(null, null), SagaMethod.class);
        return resolveByMethodRef(sagaMethod);
    }

    @Override
    public <PARENT_INPUT, OUTPUT> SagaOperation resolveRevert(
            RevertibleBiConsumer<PARENT_INPUT, OUTPUT> revertOperation) {
        SagaRevertMethod sagaRevertMethod = sagaMethodByRunnable(
                () -> revertOperation.apply(null, null, null),
                SagaRevertMethod.class
        );
        return resolveByMethodRef(sagaRevertMethod);
    }

    @Override
    public <INPUT, PARENT_INPUT, OUTPUT> SagaOperation resolveRevert(
            RevertibleThreeConsumer<PARENT_INPUT, INPUT, OUTPUT> revertOperation) {
        SagaRevertMethod sagaRevertMethod = sagaMethodByRunnable(
                () -> revertOperation.apply(null, null, null, null),
                SagaRevertMethod.class
        );
        return resolveByMethodRef(sagaRevertMethod);
    }

    @Override
    public TaskDef<SagaPipelineContext> resolveByTaskName(String taskName) {
        return taskRegistryService.<SagaPipelineContext>getRegisteredLocalTaskDef(taskName)
                .orElseThrow(() -> new SagaTaskNotFoundException(taskName));
    }

    //todo: check that all arguments have been passed to!!!
    private <A extends Annotation> A sagaMethodByRunnable(Runnable runnable, Class<? extends A> annotationCls) {
        sagaContextDiscovery.beginDetection(annotationCls);
        try {
            runnable.run();
            return sagaContextDiscovery.getSagaMethod();
        } catch (Exception exception) {
            throw new SagaMethodResolvingException(exception);
        } finally {
            sagaContextDiscovery.completeDetection();
        }
    }

    private SagaOperation resolveByMethodRef(SagaMethod sagaMethod) {
        var taskName = taskNameFor(sagaMethod);
        var operationTask = methodToSagaOperation.get(taskName);
        if (operationTask == null) {
            throw new SagaTaskNotFoundException(taskName);
        }
        return operationTask;
    }

    private SagaOperation resolveByMethodRef(SagaRevertMethod sagaRevertMethod) {
        var taskName = revertTaskNameFor(sagaRevertMethod);
        var revertOperationTask = revertMethodToSagaOperation.get(taskName);
        if (revertOperationTask == null) {
            throw new SagaTaskNotFoundException(taskName);
        }
        return revertOperationTask;
    }

    private static String taskNameFor(SagaMethod sagaMethodAnnotation) {
        String name = sagaMethodAnnotation.name();
        String version = "" + sagaMethodAnnotation.version();
        String exceptions = Arrays.stream(sagaMethodAnnotation.noRetryFor())
                .map(Class::getCanonicalName)
                .sorted()
                .collect(Collectors.joining(", "));

        if (exceptions.isEmpty()) {
            return String.join(TASK_NAME_DELIMITER,
                    TASK_PREFIX,
                    name,
                    version
            );
        }

        return String.join(TASK_NAME_DELIMITER,
                TASK_PREFIX,
                name,
                version,
                UUID.nameUUIDFromBytes(exceptions.getBytes(StandardCharsets.UTF_8)).toString()
        );
    }

    private static String revertTaskNameFor(SagaRevertMethod sagaRevertMethodAnnotation) {
        String name = sagaRevertMethodAnnotation.name();
        String version = "" + sagaRevertMethodAnnotation.version();

        return String.join(TASK_NAME_DELIMITER,
                TASK_REVERT_PREFIX,
                name,
                version
        );
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        registerSagaMethodIfExists(bean);
        registerSagaRevertMethodIfExists(bean);
        return bean;
    }

    private void registerSagaMethodIfExists(Object bean) {
        Arrays.stream(ReflectionUtils.getAllDeclaredMethods(bean.getClass()))
                .filter(method -> ReflectionHelper.findAnnotation(method, SagaMethod.class).isPresent())
                .forEach(method -> {
                    @SuppressWarnings("OptionalGetWithoutIsPresent")
                    SagaMethod sagaMethodAnnotation = ReflectionHelper.findAnnotation(method, SagaMethod.class).get();
                    var taskName = taskNameFor(sagaMethodAnnotation);
                    if (methodToSagaOperation.containsKey(taskName)) {
                        throw new SagaMethodDuplicateException(taskName);
                    }

                    var taskDef = TaskDef.privateTaskDef(taskName, SagaPipelineContext.class);
                    methodToSagaOperation.put(taskName, new SagaOperation(method, taskDef));
                    sagaContextDiscovery.registerMethod(method.toString(), sagaMethodAnnotation);

                    SagaTask sagaTask = sagaTaskFactory.sagaTask(
                            taskDef,
                            method,
                            bean,
                            sagaMethodAnnotation
                    );
                    var taskSettings = buildTaskSettings(method);
                    distributedTaskService.registerTask(sagaTask, taskSettings);
                });
    }

    private void registerSagaRevertMethodIfExists(Object bean) {
        Arrays.stream(ReflectionUtils.getAllDeclaredMethods(bean.getClass()))
                .filter(method -> ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).isPresent())
                .forEach(method -> {
                    @SuppressWarnings("OptionalGetWithoutIsPresent")
                    SagaRevertMethod sagaRevertMethodAnnotation = ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).get();
                    var revertTaskName = revertTaskNameFor(sagaRevertMethodAnnotation);
                    var taskDef = TaskDef.privateTaskDef(revertTaskName, SagaPipelineContext.class);
                    if (revertMethodToSagaOperation.containsKey(revertTaskName)) {
                        throw new SagaMethodDuplicateException(revertTaskName);
                    }
                    revertMethodToSagaOperation.put(revertTaskName, new SagaOperation(method, taskDef));
                    sagaContextDiscovery.registerMethod(method.toString(), sagaRevertMethodAnnotation);

                    SagaRevertTask sagaRevertTask = sagaTaskFactory.sagaRevertTask(
                            taskDef,
                            method,
                            bean
                    );
                    var taskSettings = buildTaskSettings(method);
                    distributedTaskService.registerTask(sagaRevertTask, taskSettings);
                });
    }

    private TaskSettings buildTaskSettings(Method method) {
        return ReflectionHelper.findAnnotation(method, Transactional.class)
                .filter(transactional -> StringUtils.isBlank(transactional.transactionManager()))
                .map(transactional -> TaskSettings.DEFAULT.toBuilder()
                        .executionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
                        .build()
                )
                .orElse(TaskSettings.DEFAULT);
    }
}

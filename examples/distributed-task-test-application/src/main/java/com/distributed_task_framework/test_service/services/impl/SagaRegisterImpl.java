package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.annotations.SagaRevertMethod;
import com.distributed_task_framework.test_service.exceptions.SagaMethodDuplicateException;
import com.distributed_task_framework.test_service.exceptions.SagaMethodResolvingException;
import com.distributed_task_framework.test_service.exceptions.SagaTaskNotFoundException;
import com.distributed_task_framework.test_service.models.SagaPipelineContext;
import com.distributed_task_framework.test_service.services.RevertibleBiConsumer;
import com.distributed_task_framework.test_service.services.SagaContextDiscovery;
import com.distributed_task_framework.test_service.services.SagaRegister;
import com.distributed_task_framework.test_service.services.SagaTaskFactory;
import com.distributed_task_framework.test_service.services.RevertibleThreeConsumer;
import com.distributed_task_framework.test_service.utils.ReflectionHelper;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component //todo: BeanPostProcessor doesn't work if create bean from method!
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaRegisterImpl implements SagaRegister, BeanPostProcessor {
    private static final String TASK_PREFIX = "_____SAGA";
    private static final String TASK_REVERT_PREFIX = "_____SAGA_REVERT";
    private static final String TASK_NAME_DELIMITER = "_";

    Map<String, TaskDef<SagaPipelineContext>> methodToTask = Maps.newHashMap();
    Map<String, TaskDef<SagaPipelineContext>> revertMethodToTask = Maps.newHashMap();
    DistributedTaskService distributedTaskService;
    TaskRegistryService taskRegistryService;
    SagaContextDiscovery sagaContextDiscovery;
    SagaTaskFactory sagaTaskFactory;

    @Override
    public <IN, OUT> TaskDef<SagaPipelineContext> resolve(Function<IN, OUT> operation) {
        SagaMethod sagaMethod = sagaMethodByRunnable(() -> operation.apply(null), SagaMethod.class, operation);
        return resolveByMethodRef(sagaMethod);
    }

    @Override
    public <IN> TaskDef<SagaPipelineContext> resolve(Consumer<IN> operation) {
        SagaMethod sagaMethod = sagaMethodByRunnable(() -> operation.accept(null), SagaMethod.class, operation);
        return resolveByMethodRef(sagaMethod);
    }

    @Override
    public <T, U, R> TaskDef<SagaPipelineContext> resolve(BiFunction<T, U, R> operation) {
        SagaMethod sagaMethod = sagaMethodByRunnable(() -> operation.apply(null, null), SagaMethod.class, operation);
        return resolveByMethodRef(sagaMethod);
    }

    @Override
    public <PARENT_INPUT, OUTPUT> TaskDef<SagaPipelineContext> resolveRevert(
            RevertibleBiConsumer<PARENT_INPUT, OUTPUT> revertOperation) {
        SagaRevertMethod sagaRevertMethod = sagaMethodByRunnable(
                () -> revertOperation.apply(null, null, null),
                SagaRevertMethod.class,
                revertOperation
        );
        return resolveByMethodRef(sagaRevertMethod);
    }

    @Override
    public <INPUT, PARENT_INPUT, OUTPUT> TaskDef<SagaPipelineContext> resolveRevert(
            RevertibleThreeConsumer<PARENT_INPUT, INPUT, OUTPUT> revertOperation) {
        SagaRevertMethod sagaRevertMethod = sagaMethodByRunnable(
                () -> revertOperation.apply(null, null, null, null),
                SagaRevertMethod.class,
                revertOperation
        );
        return resolveByMethodRef(sagaRevertMethod);
    }

    @Override
    public TaskDef<SagaPipelineContext> resolveByTaskName(String taskName) {
        return taskRegistryService.<SagaPipelineContext>getRegisteredLocalTaskDef(taskName)
                .orElseThrow(() -> new SagaTaskNotFoundException(taskName));
    }

    //todo: check that all arguments have been passed to!!!
    private <A extends Annotation> A sagaMethodByRunnable(Runnable runnable,
                                                          Class<? extends A> annotationCls,
                                                          Object methodRef) {
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

    private TaskDef<SagaPipelineContext> resolveByMethodRef(SagaMethod sagaMethod) {
        var taskName = taskNameFor(sagaMethod);
        var operationTask = methodToTask.get(taskName);
        if (operationTask == null) {
            throw new SagaTaskNotFoundException(taskName);
        }
        return operationTask;
    }

    private TaskDef<SagaPipelineContext> resolveByMethodRef(SagaRevertMethod sagaRevertMethod) {
        var taskName = revertTaskNameFor(sagaRevertMethod);
        var revertOperationTask = revertMethodToTask.get(taskName);
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
                    if (methodToTask.containsKey(taskName)) {
                        throw new SagaMethodDuplicateException(taskName);
                    }

                    var taskDef = TaskDef.privateTaskDef(taskName, SagaPipelineContext.class);
                    methodToTask.put(taskName, taskDef);
                    sagaContextDiscovery.registerMethod(method.toString(), sagaMethodAnnotation);

                    SagaTask sagaTask = sagaTaskFactory.sagaTask(
                            taskDef,
                            method,
                            bean,
                            sagaMethodAnnotation
                    );
                    distributedTaskService.registerTask(sagaTask);
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
                    if (revertMethodToTask.containsKey(revertTaskName)) {
                        throw new SagaMethodDuplicateException(revertTaskName);
                    }
                    revertMethodToTask.put(revertTaskName, taskDef);
                    sagaContextDiscovery.registerMethod(method.toString(), sagaRevertMethodAnnotation);

                    SagaRevertTask sagaRevertTask = sagaTaskFactory.sagaRevertTask(
                            taskDef,
                            method,
                            bean
                    );
                    distributedTaskService.registerTask(sagaRevertTask);
                });
    }
}

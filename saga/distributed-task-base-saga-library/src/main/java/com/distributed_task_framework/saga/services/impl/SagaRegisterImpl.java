package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMapper;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMerger;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.annotations.SagaMethod;
import com.distributed_task_framework.saga.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.configurations.SagaConfiguration;
import com.distributed_task_framework.saga.exceptions.SagaMethodDuplicateException;
import com.distributed_task_framework.saga.exceptions.SagaMethodResolvingException;
import com.distributed_task_framework.saga.exceptions.SagaTaskNotFoundException;
import com.distributed_task_framework.saga.mappers.SagaMethodPropertiesMapper;
import com.distributed_task_framework.saga.models.SagaEmbeddedPipelineContext;
import com.distributed_task_framework.saga.models.SagaOperation;
import com.distributed_task_framework.saga.services.RevertibleBiConsumer;
import com.distributed_task_framework.saga.services.RevertibleThreeConsumer;
import com.distributed_task_framework.saga.services.SagaContextDiscovery;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.services.SagaTaskFactory;
import com.distributed_task_framework.saga.utils.SagaNamingUtils;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.utils.ReflectionHelper;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ReflectionUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaRegisterImpl implements SagaRegister, BeanPostProcessor {
    Map<String, SagaOperation> methodToSagaOperation = Maps.newHashMap();
    Map<String, SagaOperation> revertMethodToSagaOperation = Maps.newHashMap();
    DistributedTaskService distributedTaskService;
    TaskRegistryService taskRegistryService;
    SagaContextDiscovery sagaContextDiscovery;
    SagaTaskFactory sagaTaskFactory;
    SagaConfiguration sagaConfiguration;
    DistributedTaskProperties properties;
    DistributedTaskPropertiesMapper distributedTaskPropertiesMapper;
    DistributedTaskPropertiesMerger distributedTaskPropertiesMerger;
    SagaMethodPropertiesMapper sagaMethodPropertiesMapper;

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
    public TaskDef<SagaEmbeddedPipelineContext> resolveByTaskName(String taskName) {
        return taskRegistryService.<SagaEmbeddedPipelineContext>getRegisteredLocalTaskDef(taskName)
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
        var taskName = SagaNamingUtils.taskNameFor(sagaMethod);
        var operationTask = methodToSagaOperation.get(taskName);
        if (operationTask == null) {
            throw new SagaTaskNotFoundException(taskName);
        }
        return operationTask;
    }

    private SagaOperation resolveByMethodRef(SagaRevertMethod sagaRevertMethod) {
        var taskName = SagaNamingUtils.taskNameFor(sagaRevertMethod);
        var revertOperationTask = revertMethodToSagaOperation.get(taskName);
        if (revertOperationTask == null) {
            throw new SagaTaskNotFoundException(taskName);
        }
        return revertOperationTask;
    }

    @SuppressWarnings("NullableProblems")
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
                var taskName = SagaNamingUtils.taskNameFor(method);
                if (methodToSagaOperation.containsKey(taskName)) {
                    throw new SagaMethodDuplicateException(taskName);
                }

                var taskDef = TaskDef.privateTaskDef(taskName, SagaEmbeddedPipelineContext.class);
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
                var revertTaskName = SagaNamingUtils.taskNameFor(method);
                var taskDef = TaskDef.privateTaskDef(revertTaskName, SagaEmbeddedPipelineContext.class);
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
        var taskPropertiesGroup = Optional.ofNullable(properties.getTaskPropertiesGroup());
        var sagaMethodPropertiesGroup = Optional.ofNullable(sagaConfiguration.getSagaMethodPropertiesGroup());

        var dtfDefaultCodeTaskProperties = distributedTaskPropertiesMapper.map(TaskSettings.DEFAULT);
        var sagaCustomCodeTaskProperties = fillCustomProperties(method);

        var dtfDefaultConfTaskProperties = taskPropertiesGroup
            .map(DistributedTaskProperties.TaskPropertiesGroup::getDefaultProperties)
            .orElse(null);
        var sagaDefaultConfTaskProperties = sagaMethodPropertiesGroup
            .map(SagaConfiguration.SagaMethodPropertiesGroup::getDefaultSagaMethodProperties)
            .map(sagaMethodPropertiesMapper::map)
            .orElse(null);
        var sagaCustomConfTaskProperties = sagaMethodPropertiesGroup
            .map(SagaConfiguration.SagaMethodPropertiesGroup::getSagaMethodProperties)
            .map(sagaMethodProperties -> sagaMethodProperties.get(SagaNamingUtils.sagaMethodNameFor(method)))
            .map(sagaMethodPropertiesMapper::map)
            .orElse(null);

        var dtfDefaultTaskProperties = distributedTaskPropertiesMerger.merge(
            dtfDefaultCodeTaskProperties,
            dtfDefaultConfTaskProperties
        );
        var sagaDefaultTaskProperties = distributedTaskPropertiesMerger.merge(
            dtfDefaultTaskProperties,
            sagaDefaultConfTaskProperties
        );

        var sagaCustomTaskProperties = distributedTaskPropertiesMerger.merge(
            sagaCustomCodeTaskProperties,
            sagaCustomConfTaskProperties
        );

        var taskProperties = distributedTaskPropertiesMerger.merge(
            sagaDefaultTaskProperties,
            sagaCustomTaskProperties
        );

        return distributedTaskPropertiesMapper.map(taskProperties);
    }

    private DistributedTaskProperties.TaskProperties fillCustomProperties(Method method) {
        var taskProperties = new DistributedTaskProperties.TaskProperties();
        fillExecutionGuarantees(method, taskProperties);
        return taskProperties;
    }

    private void fillExecutionGuarantees(Method method, DistributedTaskProperties.TaskProperties taskProperties) {
        ReflectionHelper.findAnnotation(method, Transactional.class)
            .ifPresent(executionGuarantees ->
                taskProperties.setExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE.name())
            );
    }
}

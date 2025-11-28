package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.mappers.SettingsMapper;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.SagaRegisterService;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.saga.task.SagaRevertTask;
import com.distributed_task_framework.saga.task.SagaTask;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.settings.TaskSettings;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaRegisterServiceImpl implements SagaRegisterService {
    private final Map<String, SagaSettings> registeredSagaSettings = Maps.newConcurrentMap();
    private final AtomicReference<SagaSettings> defaultSagaSettings = new AtomicReference<>(SagaSettings.DEFAULT);

    DistributedTaskService distributedTaskService;
    SagaTaskFactory sagaTaskFactory;
    SagaResolver sagaResolver;
    SettingsMapper settingsMapper;

    @Override
    public void registerSagaSettings(String name, SagaSettings sagaSettings) {
        log.info("registerSagaSettings(): name=[{}], sagaSettings=[{}]", name, sagaSettings);
        registeredSagaSettings.put(name, sagaSettings);
    }

    @Override
    public void registerDefaultSagaSettings(SagaSettings sagaSettings) {
        log.info("registerDefaultSagaSettings(): sagaSettings=[{}]", sagaSettings);
        defaultSagaSettings.set(sagaSettings);
    }

    @Override
    public SagaSettings getSagaSettings(String name) {
        return registeredSagaSettings.getOrDefault(name, defaultSagaSettings.get());
    }

    @Override
    public void unregisterSagaSettings(String name) {
        log.info("unregisterSagaSettings(): name=[{}]", name);
        registeredSagaSettings.remove(name);
    }

    @Override
    public void registerSagaMethod(String name,
                                   Method method,
                                   Object object,
                                   Collection<Object> proxyWrappers,
                                   SagaMethodSettings sagaMethodSettings) {
        log.info(
            "registerSagaMethod(): name=[{}], method=[{}], object=[{}], proxyObjects=[{}], sagaMethodSettings=[{}]",
            name,
            method,
            System.identityHashCode(object),
            proxyWrappers.stream().map(System::identityHashCode).toList(),
            sagaMethodSettings
        );

        var taskDef = TaskDef.privateTaskDef(name, SagaPipeline.class);
        var sagaOperand = SagaOperand.builder()
            .method(method)
            .targetObject(object)
            .proxyWrappers(proxyWrappers)
            .taskDef(taskDef)
            .build();
        sagaResolver.registerOperand(name, sagaOperand);

        SagaTask sagaTask = sagaTaskFactory.sagaTask(
            taskDef,
            makeAccessible(method, object),
            object,
            sagaMethodSettings
        );
        TaskSettings taskSettings = settingsMapper.map(sagaMethodSettings);
        distributedTaskService.registerTask(sagaTask, taskSettings);
    }

    @Override
    public <T, R> void registerSagaMethod(String name,
                                          SagaFunction<T, R> methodRef,
                                          Object object,
                                          Collection<Object> proxyWrappers,
                                          SagaMethodSettings sagaMethodSettings) {
        Method method = sagaResolver.findMethodInObject(methodRef, object);
        registerSagaMethod(name, method, object, proxyWrappers, sagaMethodSettings);
    }

    @Override
    public void registerSagaRevertMethod(String name,
                                         Method method,
                                         Object object,
                                         Collection<Object> proxyWrappers,
                                         SagaMethodSettings sagaMethodSettings) {
        log.info(
            "registerSagaRevertMethod(): name=[{}], method=[{}], object=[{}], proxyObjects=[{}], sagaMethodSettings=[{}]",
            name,
            method,
            System.identityHashCode(object),
            proxyWrappers.stream().map(System::identityHashCode).toList(),
            sagaMethodSettings
        );

        var taskDef = TaskDef.privateTaskDef(name, SagaPipeline.class);
        var sagaOperand = SagaOperand.builder()
            .method(method)
            .targetObject(object)
            .proxyWrappers(proxyWrappers)
            .taskDef(taskDef)
            .build();
        sagaResolver.registerOperand(name, sagaOperand);

        SagaRevertTask sagaRevertTask = sagaTaskFactory.sagaRevertTask(
            taskDef,
            makeAccessible(method, object),
            object
        );
        TaskSettings taskSettings = settingsMapper.map(sagaMethodSettings);
        distributedTaskService.registerTask(sagaRevertTask, taskSettings);
    }

    @Override
    public void unregisterSagaMethod(String name) {
        log.info("unregisterSagaMethod(): name=[{}]", name);
        sagaResolver.unregisterOperand(name);

        var taskDef = TaskDef.privateTaskDef(name, SagaPipeline.class);
        distributedTaskService.unregisterTask(taskDef);
    }

    private Method makeAccessible(Method method, Object object) {
        if ((object.getClass().getModifiers() & Modifier.PUBLIC) == 0
            || (method.getModifiers() & Modifier.PUBLIC) == 0) {
            method.setAccessible(true);
        }
        return method;
    }
}

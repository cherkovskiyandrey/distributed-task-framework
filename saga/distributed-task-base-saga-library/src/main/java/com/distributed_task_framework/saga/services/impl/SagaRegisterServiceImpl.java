package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.exceptions.SagaMethodDuplicateException;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.mappers.SettingsMapper;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.SagaRegisterService;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.settings.TaskSettings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaRegisterServiceImpl implements SagaRegisterService {
    private final Map<String, Method> registeredSagaMethodNames = Maps.newConcurrentMap();
    private final Set<Method> registeredSagaMethods = Sets.newConcurrentHashSet();
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
    public void registerSagaMethod(String name, Method method, Object object, SagaMethodSettings sagaMethodSettings) {
        log.info(
            "registerSagaMethod(): name=[{}], method=[{}], object=[{}], SagaMethodSettings=[{}]",
            name,
            method,
            object,
            sagaMethodSettings
        );

        internalRegisterMethod(name, method);
        var taskDef = TaskDef.privateTaskDef(name, SagaPipeline.class);
        SagaTask sagaTask = sagaTaskFactory.sagaTask(
            taskDef,
            makeAccessible(method, object),
            object,
            sagaMethodSettings
        );
        TaskSettings taskSettings = settingsMapper.map(sagaMethodSettings);
        distributedTaskService.registerTask(sagaTask, taskSettings);
        sagaResolver.registerOperand(name, new SagaOperand(method, taskDef));
    }

    @Override
    public <T, R> void registerSagaMethod(String name,
                                          SagaFunction<T, R> methodRef,
                                          Object object,
                                          SagaMethodSettings sagaMethodSettings) {
        Method method = sagaResolver.resolveAsMethod(methodRef, object);
        registerSagaMethod(
            name,
            method,
            object,
            sagaMethodSettings
        );
    }

    @Override
    public void registerSagaRevertMethod(String name, Method method, Object object, SagaMethodSettings sagaMethodSettings) {
        log.info(
            "registerSagaRevertMethod(): name=[{}], method=[{}], object=[{}], SagaMethodSettings=[{}]",
            name,
            method,
            object,
            sagaMethodSettings
        );

        internalRegisterMethod(name, method);
        var taskDef = TaskDef.privateTaskDef(name, SagaPipeline.class);
        SagaRevertTask sagaRevertTask = sagaTaskFactory.sagaRevertTask(
            taskDef,
            makeAccessible(method, object),
            object
        );
        TaskSettings taskSettings = settingsMapper.map(sagaMethodSettings);
        distributedTaskService.registerTask(sagaRevertTask, taskSettings);
        sagaResolver.registerOperand(name, new SagaOperand(method, taskDef));

        log.info(
            "registerSagaRevertElement(): name=[{}], method=[{}], object=[{}], sagaElementSettings=[{}]",
            name,
            method,
            object,
            sagaMethodSettings
        );
    }

    @Override
    public void unregisterSagaMethod(String name) {
        log.info("unregisterSagaMethod(): name=[{}]", name);

        internalUnregisterMethod(name);
        sagaResolver.unregisterOperand(name);

        var taskDef = TaskDef.privateTaskDef(name, SagaPipeline.class);
        distributedTaskService.unregisterTask(taskDef);
    }

    private void internalRegisterMethod(String name, Method method) {
        if (registeredSagaMethodNames.putIfAbsent(name, method) != null) {
            throw new SagaMethodDuplicateException(name);
        }
        if (!registeredSagaMethods.add(method)) {
            registeredSagaMethodNames.remove(name);
            throw new SagaMethodDuplicateException(method);
        }
    }

    private void internalUnregisterMethod(String name) {
        var method = registeredSagaMethodNames.remove(name);
        if (method != null) {
            registeredSagaMethods.remove(method);
        }
    }

    private Method makeAccessible(Method method, Object object) {
        if ((object.getClass().getModifiers() & Modifier.PUBLIC) == 0
            || (method.getModifiers() & Modifier.PUBLIC) == 0) {
            method.setAccessible(true);
        }
        return method;
    }
}

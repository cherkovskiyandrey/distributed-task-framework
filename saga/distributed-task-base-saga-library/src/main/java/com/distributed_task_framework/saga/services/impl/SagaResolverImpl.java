package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.exceptions.SagaMethodDuplicateException;
import com.distributed_task_framework.saga.exceptions.SagaMethodNotFoundException;
import com.distributed_task_framework.saga.exceptions.SagaMethodResolvingException;
import com.distributed_task_framework.saga.exceptions.SagaTaskNotFoundException;
import com.distributed_task_framework.saga.models.MethodReference;
import com.distributed_task_framework.saga.models.SagaMethod;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.utils.MethodSagaMethodFactory;
import com.distributed_task_framework.saga.utils.ReflectionHelper;
import com.distributed_task_framework.saga.utils.SerializableLambdaSagaMethodFactory;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaResolverImpl implements SagaResolver {
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    Map<String, SagaOperand> nameToSagaOperand = Maps.newHashMap();
    //we need to compare object identity only
    Table<Object, SagaMethod, String> targetObjectAndSagaMethodToName = Tables.newCustomTable(
        Maps.newIdentityHashMap(),
        Maps::newHashMap
    );
    TaskRegistryService taskRegistryService;

    @Override
    public void registerOperand(String name, SagaOperand sagaOperand) {
        log.info("registerOperand(): name=[{}], sagaOperand=[{}]", name, sagaOperand);
        var writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            if (nameToSagaOperand.containsKey(name)) {
                throw new SagaMethodDuplicateException(name);
            }
            var objects = ImmutableList.builder()
                .add(sagaOperand.getTargetObject())
                .addAll(sagaOperand.getProxyWrappers())
                .build();

            var sagaMethod = MethodSagaMethodFactory.of(sagaOperand.getMethod());
            boolean isAlreadyContains = objects.stream()
                .anyMatch(object -> targetObjectAndSagaMethodToName.contains(
                        object,
                        sagaMethod
                    )
                );
            if (isAlreadyContains) {
                throw new SagaMethodDuplicateException(sagaOperand.getMethod());
            }

            nameToSagaOperand.put(name, sagaOperand);

            var overrideSagaMethods = findAllOverrideSagaMethods(sagaOperand);
            objects.forEach(object ->
                overrideSagaMethods
                    .forEach(sm -> targetObjectAndSagaMethodToName.put(object, sm, name))
            );
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void unregisterOperand(String name) {
        log.info("unregisterOperand(): name=[{}]", name);

        SagaOperand sagaOperand;
        var writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            sagaOperand = nameToSagaOperand.remove(name);
            if (sagaOperand == null) {
                return;
            }

            var objects = ImmutableList.builder()
                .add(sagaOperand.getTargetObject())
                .addAll(sagaOperand.getProxyWrappers())
                .build();

            var overrideSagaMethods = findAllOverrideSagaMethods(sagaOperand);
            objects.forEach(object ->
                overrideSagaMethods
                    .forEach(sm -> targetObjectAndSagaMethodToName.remove(object, sm))
            );
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Collection<String> getAllRegisteredOperandNames() {
        var readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            return Sets.newHashSet(nameToSagaOperand.keySet());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public <T extends Serializable> SagaOperand resolveAsOperand(T operation) {
        log.info("resolveAsOperand(): resolving operation=[{}]", operation);
        var methodReference = parseMethodReference(operation);
        var readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            var name = targetObjectAndSagaMethodToName.get(
                methodReference.targetObject(),
                methodReference.sagaMethod()
            );
            if (name == null) {
                throw new SagaMethodNotFoundException(methodReference.toString());
            }
            var operand = nameToSagaOperand.get(name);
            if (operand == null) {
                throw new SagaMethodNotFoundException(name);
            }
            return operand;
        } finally {
            readLock.unlock();
        }
    }

    private List<SagaMethod> findAllOverrideSagaMethods(SagaOperand sagaOperand) {
        return ReflectionHelper.findAllMethodsForLastOverride(
                sagaOperand.getMethod(),
                sagaOperand.getTargetObject().getClass()
            ).stream()
            .map(MethodSagaMethodFactory::of)
            .toList();
    }

    @Override
    public <T extends Serializable> Method findMethodInObject(T methodRef, Object anchorObject) {
        var methodReference = parseMethodReference(methodRef);
        var sagaMethod = methodReference.sagaMethod();
        return ReflectionHelper.allMethods(anchorObject.getClass())
            .filter(m -> Objects.equals(MethodSagaMethodFactory.of(m), sagaMethod))
            .findFirst()
            .stream()
            .peek(method -> log.info("resolveAsMethod(): methodRef=[{}], anchorObject=[{}]", methodRef, anchorObject))
            .findAny()
            .orElseThrow(() -> new SagaMethodNotFoundException("signature=[%s] in anchorObject=[%s]".formatted(sagaMethod, anchorObject)));
    }

    private MethodReference parseMethodReference(Serializable operation) {
        try {
            Class<?> cls = operation.getClass();
            Method m = cls.getDeclaredMethod("writeReplace");
            m.setAccessible(true);
            Object replacement = m.invoke(operation);
            if (!(replacement instanceof SerializedLambda serializedLambda)) {
                throw new SagaMethodResolvingException("Can't resolve for [%s], isn't SerializedLambda".formatted(operation));
            }
            if (serializedLambda.getCapturedArgCount() == 0) {
                throw new SagaMethodResolvingException("Can't resolve for [%s], target object is undefined".formatted(operation));
            }

            return new MethodReference(
                serializedLambda.getCapturedArg(0),
                SerializableLambdaSagaMethodFactory.of(serializedLambda)
            );
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new SagaMethodResolvingException(e);
        }
    }

    @Override
    public TaskDef<SagaPipeline> resolveByTaskName(String taskName) {
        return taskRegistryService.<SagaPipeline>getRegisteredLocalTaskDef(taskName)
            .orElseThrow(() -> new SagaTaskNotFoundException(taskName));
    }

    @Override
    public Optional<TaskDef<SagaPipeline>> resolveByTaskNameIfExists(String taskName) {
        return taskRegistryService.getRegisteredLocalTaskDef(taskName);
    }

    @Override
    public TaskDef<SagaPipeline> resolveByTaskNameInCluster(String taskName) {
        if (taskRegistryService.hasClusterRegisteredTaskByName(taskName)) {
            return TaskDef.privateTaskDef(taskName, SagaPipeline.class);
        }
        throw new SagaTaskNotFoundException(taskName);
    }
}

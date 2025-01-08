package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.exceptions.SagaMethodNotFoundException;
import com.distributed_task_framework.saga.exceptions.SagaMethodResolvingException;
import com.distributed_task_framework.saga.exceptions.SagaTaskNotFoundException;
import com.distributed_task_framework.saga.models.SagaMethod;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.utils.MethodSagaMethodFactory;
import com.distributed_task_framework.saga.utils.SerializableLambdaSagaMethodFactory;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaResolverImpl implements SagaResolver {
    Map<String, SagaOperand> methodNameToSagaOperand = Maps.newConcurrentMap();
    Map<SagaMethod, String> sagaMethodToMethodName = Maps.newConcurrentMap();
    WeakHashMap<Serializable, SagaOperand> cache = new WeakHashMap<>();
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    TaskRegistryService taskRegistryService;

    @Override
    public void registerOperand(String name, SagaOperand sagaOperand) {
        log.info("registerOperand(): name=[{}], sagaOperand=[{}]", name, sagaOperand);
        methodNameToSagaOperand.put(name, sagaOperand);
        sagaMethodToMethodName.put(MethodSagaMethodFactory.of(sagaOperand.getMethod()), name);
    }

    @Override
    public void unregisterOperand(String name) {
        log.info("unregisterOperand(): name=[{}]", name);
        var sagaOperand = methodNameToSagaOperand.remove(name);
        sagaMethodToMethodName.remove(MethodSagaMethodFactory.of(sagaOperand.getMethod()));
    }

    @Override
    public <T extends Serializable> SagaOperand resolveAsOperand(T operation) {
        var readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            var operand = cache.get(operation);
            if (operand != null) {
                return operand;
            }
        } finally {
            readLock.unlock();
        }

        var writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            var operand = cache.get(operation);
            if (operand != null) {
                return operand;
            }

            var sagaMethod = lookupSagaMethod(operation);
            var name = sagaMethodToMethodName.get(sagaMethod);
            if (name == null) {
                throw new SagaMethodNotFoundException(sagaMethod.toString());
            }
            operand = methodNameToSagaOperand.get(name);
            if (operand == null) {
                throw new SagaMethodNotFoundException(name);
            }
            cache.put(operation, operand);
            log.info("resolveAsOperand(): operation=[{}] => operand=[{}]", operation, operand);
            return operand;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public <T extends Serializable> Method resolveAsMethod(T methodRef, Object anchorObject) {
        var sagaMethod = lookupSagaMethod(methodRef);
        for (var cls = anchorObject.getClass(); cls != null; cls = cls.getSuperclass()) {
            var method = Arrays.stream(anchorObject.getClass().getDeclaredMethods())
                .filter(m -> Objects.equals(MethodSagaMethodFactory.of(m), sagaMethod))
                .findFirst()
                .orElse(null);
            if (method != null) {
                log.info("resolveAsMethod(): methodRef=[{}], anchorObject=[{}]", methodRef, anchorObject);
                return method;
            }
        }
        throw new SagaMethodNotFoundException("signature=[%s] in anchorObject=[%s]".formatted(sagaMethod, anchorObject));
    }

    private SagaMethod lookupSagaMethod(Serializable operation) {
        try {
            Class<?> cls = operation.getClass();
            Method m = cls.getDeclaredMethod("writeReplace");
            m.setAccessible(true);
            Object replacement = m.invoke(operation);
            if (!(replacement instanceof SerializedLambda serializedLambda)) {
                throw new SagaMethodResolvingException("Can't resolve for [%s]".formatted(operation));
            }
            return SerializableLambdaSagaMethodFactory.of(serializedLambda);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new SagaMethodResolvingException(e);
        }
    }

    @Override
    public TaskDef<SagaPipeline> resolveByTaskName(String taskName) {
        return taskRegistryService.<SagaPipeline>getRegisteredLocalTaskDef(taskName)
            .orElseThrow(() -> new SagaTaskNotFoundException(taskName));
    }
}

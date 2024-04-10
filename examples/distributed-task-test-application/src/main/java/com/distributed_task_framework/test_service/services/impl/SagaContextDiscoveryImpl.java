package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.exceptions.SagaMethodNotFoundException;
import com.distributed_task_framework.test_service.services.SagaContextDiscovery;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Aspect
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaContextDiscoveryImpl implements SagaContextDiscovery {
    ConcurrentMap<String, SagaMethod> registeredMethods = Maps.newConcurrentMap();
    ThreadLocal<Boolean> requested = new ThreadLocal<>();
    ThreadLocal<String> methodSignature = new ThreadLocal<>();
    ThreadLocal<SagaMethod> sagaMethodRef = new ThreadLocal<>();

    @Pointcut("@annotation(com.distributed_task_framework.test_service.annotations.SagaMethod)")
    public void methodWithSagaElement() {
    }

    @Around("methodWithSagaElement()")
    public Object aroundCallSagaElement(ProceedingJoinPoint pjp) throws Throwable {
        if (isRequest()) {
            var sig = pjp.getSignature().toLongString();
            SagaMethod sagaMethodAnnotation = Optional.ofNullable(registeredMethods.get(sig))
                    .orElseThrow(() -> new SagaMethodNotFoundException(sig));

            methodSignature.set(sig);
            sagaMethodRef.set(sagaMethodAnnotation);

            return null;
        }
        return pjp.proceed();
    }

    @Override
    public void registerMethod(String methodName, SagaMethod sagaMethodAnnotation) {
        registeredMethods.put(methodName, sagaMethodAnnotation);
    }

    @Override
    public void beginDetection() {
        sagaMethodRef.set(null);
        methodSignature.set(null);
        requested.set(true);
    }

    @Override
    public SagaMethod getSagaMethod() {
        if (!isRequest() || sagaMethodRef.get() == null) {
            throw new SagaMethodNotFoundException(methodSignature.get());
        }
        return sagaMethodRef.get();
    }

    @Override
    public void completeDetection() {
        sagaMethodRef.set(null);
        methodSignature.set(null);
        requested.set(false);
    }

    private boolean isRequest() {
        return Boolean.TRUE.equals(requested.get());
    }
}

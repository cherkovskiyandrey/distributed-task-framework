package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.test_service.exceptions.SagaException;
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

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Aspect
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaContextDiscoveryImpl implements SagaContextDiscovery {
    ConcurrentMap<String, Annotation> registeredMethods = Maps.newConcurrentMap();
    ThreadLocal<Class<? extends Annotation>> request = new ThreadLocal<>();
    ThreadLocal<String> methodSignature = new ThreadLocal<>();
    ThreadLocal<Annotation> sagaMethodRef = new ThreadLocal<>();

    @Pointcut("@annotation(com.distributed_task_framework.test_service.annotations.SagaMethod)")
    public void methodWithSagaElement() {
    }

    @Pointcut("@annotation(com.distributed_task_framework.test_service.annotations.SagaRevertMethod)")
    public void methodWithSagaRevertElement() {
    }

    @Around("methodWithSagaElement() || methodWithSagaRevertElement()")
    public Object aroundCallSagaElement(ProceedingJoinPoint pjp) throws Throwable {
        if (isRequest()) {
            var sig = pjp.getSignature().toLongString();
            Annotation annotation = Optional.ofNullable(registeredMethods.get(sig))
                    .filter(ann -> request.get().equals(ann.getClass()))
                    .orElseThrow(() -> new SagaMethodNotFoundException(
                                    "Method=[%s] isn't marked as [%s]".formatted(
                                            request.get(),
                                            sig
                                    )
                            )
                    );
            methodSignature.set(sig);
            sagaMethodRef.set(annotation);

            return null;
        }
        return pjp.proceed();
    }

    @Override
    public <SAGA_METHOD extends Annotation> void registerMethod(String methodName, SAGA_METHOD sagaMethod) {
        registeredMethods.put(methodName, sagaMethod);
    }

    @Override
    public <SAGA_METHOD extends Annotation> void beginDetection(Class<SAGA_METHOD> sagaMethodClass) {
        sagaMethodRef.set(null);
        methodSignature.set(null);
        request.set(sagaMethodClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <SAGA_METHOD extends Annotation> SAGA_METHOD getSagaMethod() {
        if (!isRequest() || sagaMethodRef.get() == null) {
            throw new SagaException("Unexpected call for method=[%s]".formatted(methodSignature.get()));
        }
        return (SAGA_METHOD) sagaMethodRef.get();
    }

    @Override
    public void completeDetection() {
        sagaMethodRef.set(null);
        methodSignature.set(null);
        request.set(null);
    }

    private boolean isRequest() {
        return request.get() != null;
    }
}
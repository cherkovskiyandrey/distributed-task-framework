package com.distributed_task_framework.saga.autoconfigure.utils;

import lombok.experimental.UtilityClass;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.util.Assert;

import java.util.Collection;

@UtilityClass
public class ReflectionHelper {

    public record ProxyObject(
        Object targetObject,
        Collection<Object> proxyWrappers
    ) {
    }

    //todo
    public ProxyObject unwrapSpringBean(Object bean) {
        throw new UnsupportedOperationException("todo: implement");
    }


    //todo
    @SuppressWarnings("unchecked")
    public static <T> T getUltimateTargetObject(Object candidate) {
        Assert.notNull(candidate, "Candidate must not be null");
        try {
            if (AopUtils.isAopProxy(candidate) && candidate instanceof Advised advised) {
                Object target = advised.getTargetSource().getTarget();
                if (target != null) {
                    return (T) getUltimateTargetObject(target);
                }
            }
        } catch (Throwable ex) {
            throw new IllegalStateException("Failed to unwrap proxied object", ex);
        }
        return (T) candidate;
    }
}

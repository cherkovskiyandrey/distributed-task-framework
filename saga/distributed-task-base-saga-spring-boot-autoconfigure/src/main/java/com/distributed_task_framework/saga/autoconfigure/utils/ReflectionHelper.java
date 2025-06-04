package com.distributed_task_framework.saga.autoconfigure.utils;

import com.google.common.collect.Lists;
import lombok.experimental.UtilityClass;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;

import java.util.Collection;

@UtilityClass
public class ReflectionHelper {

    public record ProxyObject(
        Object targetObject,
        Collection<Object> proxyWrappers
    ) {
    }

    /**
     * Unwrap any spring bean (JDK dynamic proxy or CGLIB proxy) recursively.
     *
     * @param bean
     * @return ProxyObject
     * @throws IllegalStateException if target object can't be resolved
     */
    public ProxyObject unwrapSpringBean(Object bean) {
        var objectSequence = Lists.newArrayList();

        try {
            while (bean != null) {
                objectSequence.add(bean);
                if (AopUtils.isAopProxy(bean) && bean instanceof Advised advised) {
                    bean = advised.getTargetSource().getTarget();
                } else {
                    break;
                }
            }
        } catch (Throwable ex) {
            throw new IllegalStateException("Failed to unwrap proxied object", ex);
        }

        return new ProxyObject(
            objectSequence.get(objectSequence.size() - 1),
            Lists.newArrayList(objectSequence.subList(0, objectSequence.size() - 1))
        );
    }
}

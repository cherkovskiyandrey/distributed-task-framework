package com.distributed_task_framework.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class SpringGraphHelper {


    @SuppressWarnings("unchecked")
    public static <T> List<SimpleServiceDefinition<T>> singletonTopologyReverseOrderByType(ConfigurableListableBeanFactory beanFactory,
                                                                                           Class<T> cls) {
        List<SimpleServiceDefinition<T>> result = Lists.newArrayList();

        traverseGraphInTopologyOrder(
            beanFactory,
            serviceName -> {
                var singleton = beanFactory.getSingleton(serviceName);
                if (singleton != null && cls.isAssignableFrom(singleton.getClass())) {
                    T service = (T)singleton;
                    result.add(SimpleServiceDefinition.of(serviceName, service));
                }
            }
        );

        return result;
    }

    private static void traverseGraphInTopologyOrder(ConfigurableListableBeanFactory beanFactory,
                                                     Consumer<String> beanNameConsumer) {
        Set<String> visitedSingletons = Sets.newHashSet();

        // According to spring sources:
        // beanFactory.getSingletonNames is set of registered singletons, containing the bean names in registration order.
        var singletonNames = beanFactory.getSingletonNames();
        if (singletonNames.length == 0) {
            return;
        }

        for (int i = singletonNames.length - 1; i >= 0; i--) {
            String rootSingleton = singletonNames[i];
            if (visitedSingletons.contains(rootSingleton)) {
                // already handled
                continue;
            }

            LinkedList<String> stack = Lists.newLinkedList();
            stack.addLast(rootSingleton);
            Set<String> visitingSingletons = Sets.newHashSet();
            while (!stack.isEmpty()) {
                var singleton = stack.peekLast();
                if (visitedSingletons.contains(singleton)) {
                    // already processed
                    stack.removeLast();
                    continue;
                }
                visitingSingletons.add(singleton);

                var depOnSingletons = Sets.newHashSet(beanFactory.getDependentBeans(singleton));
                // remove visited singletons
                var notVisitedSingletons = Sets.difference(depOnSingletons, visitedSingletons);
                // remove cycles
                notVisitedSingletons = Sets.difference(notVisitedSingletons, visitingSingletons);
                if (notVisitedSingletons.isEmpty()) {
                    stack.removeLast();
                    visitingSingletons.remove(singleton);
                    visitedSingletons.add(singleton);
                    beanNameConsumer.accept(singleton);
                    continue;
                }
                notVisitedSingletons.forEach(stack::addLast);
            }
        }
    }

    @Value(staticConstructor = "of")
    public static class SimpleServiceDefinition<T> {
        String serviceName;
        @EqualsAndHashCode.Exclude
        T service;
    }
}

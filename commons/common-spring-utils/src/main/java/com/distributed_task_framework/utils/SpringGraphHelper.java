package com.distributed_task_framework.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

public class SpringGraphHelper {


    // TODO: идея:
    // 1. обходим beanFactory.getSingletonNames(); с конца так как: Set of registered singletons, containing the bean names in registration order. !!!!!
    // 2. берем ноду, смотрим beanFactory.getDependentBeans(singleton)
    // 3. Запихиваем их всех в стэк
    // 4. Если пусто в beanFactory.getDependentBeans(singleton) - добавляем ноду в сет удаленных и удаляем ее
    // 5. обрабатываем следующую в стэке
    // 6. Если встретили цикл - разруливаем через visitingSingletons
    // 7. Из beanFactory.getDependentBeans(singleton) выкидываем те что уже удалили

    public static <T> List<SimpleServiceDefinition<T>> topologyReverseOrderByType(ConfigurableListableBeanFactory beanFactory,
                                                                                  Class<T> cls) {
        List<SimpleServiceDefinition<T>> result = Lists.newArrayList();

        traverseGraphInTopologyOrder(
            beanFactory,
            (serviceName, depsOnServiceNames) -> {
                depsOnNumberToServiceName.putIfAbsent(serviceName, 0);
                depsOnServiceNames.forEach(sn -> depsOnNumberToServiceName.compute(sn,
                    (k, v) -> {
                        if (v == null) {
                            return 0;
                        }
                        return ++v;
                    }
                ));
                serviceNameToDepsOnServiceNames.put(serviceName, depsOnServiceNames);
            }
        );

        //todo: topology sort

        return result;
    }

    private static void traverseGraphInTopologyOrder(ConfigurableListableBeanFactory beanFactory,
                                                     BiConsumer<String, Set<String>> beanWithDepsOnConsumer) {
        Set<String> visitedSingletons = Sets.newHashSet();

        //Set of registered singletons, containing the bean names in registration order. !!!!!
        var singletonNames = beanFactory.getSingletonNames();
        for (String rootSingleton : singletonNames) {
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
                    continue;
                }

                if (visitingSingletons.add(singleton)) {
                    // cycle in DFS
                    continue;
                }
                var depOnSingletons = Sets.newHashSet(beanFactory.getDependentBeans(singleton));
                var notVisitedSingletons = Sets.difference(depOnSingletons, visitingSingletons);
                // remove cycles
                notVisitedSingletons = Sets.difference(notVisitedSingletons, visitingSingletons);
                if (notVisitedSingletons.isEmpty()) {
                    stack.removeLast();
                    visitingSingletons.remove(singleton);
                    visitedSingletons.add(singleton);
                    continue;
                }

                beanWithDepsOnConsumer.accept(singleton, Sets.newHashSet(notVisitedSingletons));
                notVisitedSingletons.forEach(stack::addLast);
            }
        }
    }

    @Value
    public static class SimpleServiceDefinition<T> {
        String serviceName;
        @EqualsAndHashCode.Exclude
        T service;
    }
}

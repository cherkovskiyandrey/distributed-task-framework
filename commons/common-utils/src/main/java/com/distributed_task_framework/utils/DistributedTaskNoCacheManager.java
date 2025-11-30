package com.distributed_task_framework.utils;

import lombok.SneakyThrows;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;


public class DistributedTaskNoCacheManager implements DistributedTaskCacheManager {

    private static final DistributedTaskCache<Object> EMPTY_CACHE = new DistributedTaskCache<>() {

        @Override
        public Optional<Object> get(String key) {
            return Optional.empty();
        }

        @SneakyThrows
        @Override
        public Object get(String key, Callable<Object> valueLoader) {
            return valueLoader.call();
        }

        @Override
        public void invalidate() {
        }
    };

    @SuppressWarnings("unchecked")
    private <T> DistributedTaskCache<T> emptyCache() {
        return (DistributedTaskCache<T>) EMPTY_CACHE;
    }

    @Override
    public <T> Optional<DistributedTaskCache<T>> getCache(String name) {
        return Optional.empty();
    }

    @Override
    public <T> DistributedTaskCache<T> getOrCreateCache(String name, DistributedTaskCacheSettings distributedTaskCacheSettings) {
        return emptyCache();
    }

    @Override
    public Collection<String> getCacheNames() {
        return List.of();
    }
}

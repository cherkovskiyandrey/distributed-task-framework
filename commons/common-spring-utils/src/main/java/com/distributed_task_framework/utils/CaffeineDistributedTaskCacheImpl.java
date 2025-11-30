package com.distributed_task_framework.utils;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;

import java.util.Optional;
import java.util.concurrent.Callable;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CaffeineDistributedTaskCacheImpl<T> implements DistributedTaskCache<T> {
    Cache<String, T> nativeCache;

    @Override
    public Optional<T> get(String key) {
        return Optional.ofNullable(nativeCache.getIfPresent(key));
    }

    @Override
    public T get(String key, Callable<T> valueLoader) {
        return nativeCache.get(key, k -> wrapException(valueLoader));
    }

    @SneakyThrows
    private T wrapException(Callable<T> callable) {
        return callable.call();
    }

    @Override
    public void invalidate() {
        nativeCache.invalidateAll();
    }
}

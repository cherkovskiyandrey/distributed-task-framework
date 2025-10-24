package com.distributed_task_framework.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CaffeineDistributedTaskCacheManagerImpl implements DistributedTaskCacheManager {
    ConcurrentMap<String, DistributedTaskCache<?>> cacheByName = Maps.newConcurrentMap();

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<DistributedTaskCache<T>> getCache(String name) {
        return Optional.ofNullable((DistributedTaskCache<T>) cacheByName.get(name));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> DistributedTaskCache<T> getOrCreateCache(String name, DistributedTaskCacheSettings distributedTaskCacheSettings) {
        return (DistributedTaskCache<T>) cacheByName.computeIfAbsent(
            name,
            k -> wrap(createNativeCache(distributedTaskCacheSettings))
        );
    }

    private DistributedTaskCache<?> wrap(Cache<String, Object> nativeCache) {
        return new CaffeineDistributedTaskCacheImpl<>(nativeCache);
    }

    private Cache<String, Object> createNativeCache(DistributedTaskCacheSettings distributedTaskCacheSettings) {
        var cacheBuilder = Caffeine.newBuilder();
        if (distributedTaskCacheSettings.getExpireAfterWrite() != null) {
            cacheBuilder.expireAfterWrite(distributedTaskCacheSettings.getExpireAfterWrite());
        }
        if (distributedTaskCacheSettings.getExpireAfterAccess() != null) {
            cacheBuilder.expireAfterAccess(distributedTaskCacheSettings.getExpireAfterAccess());
        }
        return cacheBuilder.build();
    }

    @Override
    public Collection<String> getCacheNames() {
        return Sets.newHashSet(cacheByName.keySet());
    }
}

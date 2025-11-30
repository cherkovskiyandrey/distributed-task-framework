package com.distributed_task_framework.utils;

import java.util.Collection;
import java.util.Optional;

public interface DistributedTaskCacheManager {

    /**
     * Return cache by name if exists.
     *
     * @param name
     * @param <T>
     * @return
     */
    <T> Optional<DistributedTaskCache<T>> getCache(String name);

    /**
     * Return existed or create new one cache and associate it with name.
     * The method is thread safe.
     * Type check is only on compile level.
     *
     * @param name
     * @return
     */
    <T> DistributedTaskCache<T> getOrCreateCache(String name, DistributedTaskCacheSettings distributedTaskCacheSettings);

    /**
     * Return existed cache names.
     *
     * @return
     */
    Collection<String> getCacheNames();
}

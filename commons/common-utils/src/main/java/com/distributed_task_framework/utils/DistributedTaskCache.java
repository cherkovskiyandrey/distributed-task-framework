package com.distributed_task_framework.utils;

import java.util.Optional;
import java.util.concurrent.Callable;

public interface DistributedTaskCache<T> {

    /**
     * Return value from cache if exists.
     * Null values is prohibited.
     *
     * @param key
     * @return
     */
    Optional<T> get(String key);

    /**
     * Return value for key form cache if exists, otherwise use loader to load value to cache
     * and return it. Loading operation is synchronized.
     * Null values is prohibited.
     *
     * @param key
     * @param valueLoader
     * @return
     */
    T get(String key, Callable<T> valueLoader);

    /**
     * Invalidate the cache through removing all mappings,
     * expecting all entries to be immediately invisible for subsequent lookups.
     *
     * @return
     */
    void invalidate();
}

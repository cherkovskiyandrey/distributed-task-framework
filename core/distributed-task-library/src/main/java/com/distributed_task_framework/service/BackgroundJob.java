package com.distributed_task_framework.service;

@FunctionalInterface
public interface BackgroundJob {
    void start();
}

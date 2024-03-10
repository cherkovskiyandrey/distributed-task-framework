package com.distributed_task_framework.perf_test_app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.distributed_task_framework.perf_test")
public class PerfTestApplication {
    public static void main(String[] args) {
        SpringApplication.run(PerfTestApplication.class, args);
    }
}
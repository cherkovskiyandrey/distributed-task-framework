package com.distributed_task_framework;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;

public class Postgresql16Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @SuppressWarnings("resource")
    public static final PostgreSQLContainer<?> PG_CONTAINER = new PostgreSQLContainer<>("postgres:16-alpine")
        .withStartupTimeout(Duration.ofMinutes(1))
        .withCommand("-c max_connections=100");

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        PG_CONTAINER.start();
        TestPropertyValues.of(
            "spring.datasource.url=" + PG_CONTAINER.getJdbcUrl(),
            "spring.datasource.username=" + PG_CONTAINER.getUsername(),
            "spring.datasource.password=" + PG_CONTAINER.getPassword()
        ).applyTo(applicationContext.getEnvironment());
    }
}

package com.distributed_task_framework;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;

@Slf4j
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

        log.info(
            "initialize(): PG_URL={}, PG_USERNAME={}, PG_PASSWORD={}",
            PG_CONTAINER.getJdbcUrl(),
            PG_CONTAINER.getUsername(),
            PG_CONTAINER.getPassword()
        );
    }
}

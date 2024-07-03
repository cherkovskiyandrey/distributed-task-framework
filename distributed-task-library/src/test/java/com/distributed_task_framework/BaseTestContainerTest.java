package com.distributed_task_framework;

import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.io.File;
import java.time.Duration;

@ActiveProfiles("test")
@SpringBootTest //(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(BaseTestConfiguration.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public abstract class BaseTestContainerTest {
    protected static final String POSTGRESQL_SERVICE = "distributed-task-postgresql";
    protected static final int POSTGRESQL_PORT = 5432;

    private static final WaitStrategy WAIT_STRATEGY = Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30));

    protected static final DockerComposeContainer<?> COMPOSE_CONTAINER = new DockerComposeContainer<>(new File("./../docker-compose.yaml"))
            .withExposedService(POSTGRESQL_SERVICE, POSTGRESQL_PORT, WAIT_STRATEGY);

    static {
        COMPOSE_CONTAINER.start();
    }

    @DynamicPropertySource
    protected static void datasourceConfig(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> String.format("jdbc:postgresql://%s:%d/distributed-task",
                COMPOSE_CONTAINER.getServiceHost(POSTGRESQL_SERVICE, POSTGRESQL_PORT),
                COMPOSE_CONTAINER.getServicePort(POSTGRESQL_SERVICE, POSTGRESQL_PORT)
        ));
        registry.add("spring.datasource.password", () -> "distributed-task");
        registry.add("spring.datasource.username", () -> "distributed-task");
    }
}

package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.saga.autoconfigure.test_data.services.ExternalSagaTestService;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.utils.Postgresql16Initializer;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.saga.enabled=false",
        "distributed-task.common.app-name=test"
    }
)
@EnableAutoConfiguration
@ContextConfiguration(
    classes = {SagaAutoconfiguration.class, MappersConfiguration.class, AdditionalTestConfiguration.class},
    initializers = Postgresql16Initializer.class
)
@FieldDefaults(level = AccessLevel.PROTECTED)
@DirtiesContext // in order to prevent races between current dtf context and from BaseSpringIntegrationTest.
public class DisabledSagaIntegrationTest {
    @Autowired
    ConfigurableApplicationContext applicationContext;
    @Autowired
    ExternalSagaTestService externalSagaTestService;
    @Autowired
    DistributionSagaService distributionSagaService;

    @Test
    void shouldNotCreateUnnecessaryBeansWhenDisabled() {
        assertThatThrownBy(() -> applicationContext.getBean(SagaRepository.class))
            .isInstanceOf(NoSuchBeanDefinitionException.class);
        assertThatThrownBy(() -> applicationContext.getBean(SagaManager.class))
            .isInstanceOf(NoSuchBeanDefinitionException.class);
        assertThatThrownBy(() -> applicationContext.getBean(SagaResolver.class))
            .isInstanceOf(NoSuchBeanDefinitionException.class);
        assertThatThrownBy(() -> applicationContext.getBean(SagaTaskFactory.class))
            .isInstanceOf(NoSuchBeanDefinitionException.class);
    }

    @SneakyThrows
    @Test
    void shouldBeDisabledWhenDisabled() {
        //when
        externalSagaTestService.setValue("hello");

        //do
        var optResult = distributionSagaService.create("test")
            .registerToRun(
                externalSagaTestService::forward,
                externalSagaTestService::backward,
                "world"
            )
            .start()
            .get();

        //verify
        assertThat(optResult).isEmpty();
    }
}
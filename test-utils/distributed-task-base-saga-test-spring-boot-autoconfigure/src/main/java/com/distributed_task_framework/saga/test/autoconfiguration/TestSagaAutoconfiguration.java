package com.distributed_task_framework.saga.test.autoconfiguration;

import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.test.autoconfiguration.service.SagaTestUtil;
import com.distributed_task_framework.saga.test.autoconfiguration.service.impl.SagaTestUtilImpl;
import com.distributed_task_framework.test.autoconfigure.TestDistributedTaskAutoconfiguration;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/**
 * Special configuration to use in integration tests based on saga.
 * Differences:
 * <ol>
 *     <li>
 *         Use test optimisations for dtf from {@link TestDistributedTaskAutoconfiguration}
 *     </li>
 *     <li>
 *         There is a useful utility bean {@link SagaTestUtil} to provide method to reinit saga and underlined dtf.
 *         Should be used before run any test in order to cancel current saga flows and get rid of potential
 *         side effect from previous tests.
 *     </li>
 * </ol>
 *
 */
@Profile("test")
@ConditionalOnProperty(
    prefix = "distributed-task",
    name = {
        "enabled",
        "saga.enabled"
    },
    havingValue = "true"
)
@ConditionalOnClass(DistributionSagaService.class)
public class TestSagaAutoconfiguration {

    @Bean
    public SagaTestUtil sagaTestUtil(DistributedTaskTestUtil distributedTaskTestUtil,
                                     SagaRepository sagaRepository,
                                     DlsSagaContextRepository dlsSagaContextRepository) {
        return new SagaTestUtilImpl(
            distributedTaskTestUtil,
            sagaRepository,
            dlsSagaContextRepository
        );
    }
}

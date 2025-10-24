package com.distributed_task_framework.saga;

import com.distributed_task_framework.saga.mappers.SagaMapper;
import com.distributed_task_framework.saga.mappers.SettingsMapper;
import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.SagaRegisterService;
import com.distributed_task_framework.saga.services.impl.DistributionSagaServiceImpl;
import com.distributed_task_framework.saga.services.impl.SagaHelper;
import com.distributed_task_framework.saga.services.impl.SagaManagerImpl;
import com.distributed_task_framework.saga.services.impl.SagaRegisterServiceImpl;
import com.distributed_task_framework.saga.services.impl.SagaResolverImpl;
import com.distributed_task_framework.saga.services.impl.SagaTaskFactoryImpl;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.saga.settings.SagaCommonSettings;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.utils.DistributedTaskCacheManager;
import com.distributed_task_framework.utils.DistributedTaskNoCacheManager;
import com.distributed_task_framework.utils.TestClock;
import org.mapstruct.factory.Mappers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.repository.config.EnableJdbcAuditing;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.Clock;
import java.time.Duration;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_TX_MANAGER;


@Configuration
@EnableJdbcAuditing
@EnableJdbcRepositories(
    basePackageClasses = SagaRepository.class,
    transactionManagerRef = DTF_TX_MANAGER,
    jdbcOperationsRef = DTF_JDBC_OPS
)
@EnableTransactionManagement
public class BaseTestConfiguration {
    private static final String INTERNAL_SAGA_DISTRIBUTED_TASK_CACHE_MANAGER_NAME = "internalSagaDistributedTaskCacheManager";

    @Bean
    public TestClock sagaInternalClock() {
        return new TestClock();
    }

    @Bean
    public SagaCommonSettings sagaCommonSettings() {
        return SagaCommonSettings.buildDefault().toBuilder()
            .deprecatedSagaScanInitialDelay(Duration.ofMillis(500))
            .deprecatedSagaScanFixedDelay(Duration.ofMillis(500))
            .build();
    }

    @Bean
    @Qualifier(INTERNAL_SAGA_DISTRIBUTED_TASK_CACHE_MANAGER_NAME)
    public DistributedTaskCacheManager distributedTaskCacheManager() {
        return new DistributedTaskNoCacheManager();
    }

    @Bean
    public SettingsMapper settingsMapper() {
        return Mappers.getMapper(SettingsMapper.class);
    }

    @Bean
    public SagaMapper contextMapper() {
        return Mappers.getMapper(SagaMapper.class);
    }

    @Bean
    public SagaHelper sagaHelper(TaskSerializer taskSerializer) {
        return new SagaHelper(taskSerializer);
    }

    @Bean
    public SagaResolver sagaResolver(TaskRegistryService taskRegistryService) {
        return new SagaResolverImpl(taskRegistryService);
    }

    @Bean
    public SagaManager sagaContextService(DistributedTaskService distributedTaskService,
                                          SagaRepository sagaRepository,
                                          DlsSagaContextRepository dlsSagaContextRepository,
                                          @Qualifier(INTERNAL_SAGA_DISTRIBUTED_TASK_CACHE_MANAGER_NAME) DistributedTaskCacheManager distributedTaskCacheManager,
                                          SagaHelper sagaHelper,
                                          SagaMapper sagaMapper,
                                          @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                          Clock clock,
                                          SagaCommonSettings sagaCommonSettings) {
        return new SagaManagerImpl(
            distributedTaskService,
            sagaRepository,
            dlsSagaContextRepository,
            distributedTaskCacheManager,
            sagaHelper,
            sagaMapper,
            transactionManager,
            sagaCommonSettings,
            clock
        );
    }

    @Bean
    public SagaTaskFactory sagaTaskFactory(SagaResolver sagaResolver,
                                           DistributedTaskService distributedTaskService,
                                           TaskSerializer taskSerializer,
                                           SagaManager sagaManager,
                                           SagaHelper sagaHelper) {
        return Mockito.spy(new SagaTaskFactoryImpl(
                sagaResolver,
                distributedTaskService,
                sagaManager,
                taskSerializer,
                sagaHelper
            )
        );
    }

    @Bean
    public SagaRegisterService sagaRegisterService(DistributedTaskService distributedTaskService,
                                                   SagaTaskFactory sagaTaskFactory,
                                                   SagaResolver sagaResolver,
                                                   SettingsMapper settingsMapper) {
        return new SagaRegisterServiceImpl(
            distributedTaskService,
            sagaTaskFactory,
            sagaResolver,
            settingsMapper
        );
    }

    @Bean
    public DistributionSagaService distributionSagaService(@Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                                           SagaResolver sagaResolver,
                                                           SagaRegisterService sagaRegisterService,
                                                           DistributedTaskService distributedTaskService,
                                                           SagaManager sagaManager,
                                                           SagaHelper sagaHelper) {
        return new DistributionSagaServiceImpl(
            transactionManager,
            sagaResolver,
            sagaRegisterService,
            distributedTaskService,
            sagaManager,
            sagaHelper
        );
    }
}

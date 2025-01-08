package com.distributed_task_framework.test.autoconfigure;

import com.distributed_task_framework.autoconfigure.DistributedTaskAutoconfigure;
import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.autoconfigure.RemoteTasks;
import com.distributed_task_framework.autoconfigure.TaskConfigurationDiscoveryProcessor;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMapper;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMerger;
import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.persistence.repository.DltRepository;
import com.distributed_task_framework.persistence.repository.NodeStateRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.internal.CapabilityRegisterProvider;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test.ClusterProviderTestImpl;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import com.distributed_task_framework.test.autoconfigure.service.impl.DistributedTaskTestUtilImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;

import java.time.Clock;
import java.util.Collection;

/**
 * Special configuration to use in integration tests based on dtf.
 * Differences:
 * <ol>
 *     <li>
 *          Don't autostart cron task. As result kinda tasks can be scheduled as regular task.
 *          Very useful feature in test. Which
 *          give a chance to test cron task without waiting for time to fire based on cron.
 *          Can be managed by property: "distributed-task.test.cron.enabled". Default value is false.
 *     </li>
 *     <li>
 *         There is a mocked ClusterProvider. It get rid of extra waiting in test
 *         until node is registered to use, git rid of potential cases when
 *         node is overloaded and is excluded from planning.
 *     </li>
 *     <li>
 *         There is an useful utility bean {@link DistributedTaskTestUtil} to provide method to reinit dtf.
 *         Should be used before run any test in order to cancel current tasks and get rid of potential
 *         side effect from previous tests.
 *     </li>
 * </ol>
 */
@Profile("test")
@AutoConfiguration(before = DistributedTaskAutoconfigure.class)
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "true")
@ConditionalOnClass(DistributedTaskService.class)
public class TestDistributedTaskAutoconfiguration {

    @Bean
    public ClusterProvider clusterProvider(NodeStateRepository nodeStateRepository,
                                           @Lazy CapabilityRegisterProvider capabilityRegisterProvider,
                                           Clock clock) {
        return new ClusterProviderTestImpl(nodeStateRepository, capabilityRegisterProvider, clock);
    }

    @Bean
    public TaskConfigurationDiscoveryProcessor taskConfigurationDiscoveryProcessor(DistributedTaskProperties properties,
                                                                                   DistributedTaskService distributedTaskService,
                                                                                   DistributedTaskPropertiesMapper distributedTaskPropertiesMapper,
                                                                                   DistributedTaskPropertiesMerger distributedTaskPropertiesMerger,
                                                                                   Collection<Task<?>> tasks,
                                                                                   RemoteTasks remoteTasks,
                                                                                   @Value("${distributed-task.test.cron.enabled:false}") boolean isCronEnabled) {
        return new TaskConfigurationDiscoveryProcessor(
            properties,
            distributedTaskService,
            distributedTaskPropertiesMapper,
            distributedTaskPropertiesMerger,
            tasks,
            remoteTasks,
            (taskSettings, taskDef) -> {
                if (taskSettings.hasCron() && !isCronEnabled) {
                    return taskSettings.toBuilder()
                        .cron(null) //disable cron for test
                        .build();
                }
                return taskSettings;
            }
        );
    }

    @Bean
    public DistributedTaskTestUtil distributedTaskTestUtil(TaskRepository taskRepository,
                                                           DltRepository dltRepository,
                                                           DistributedTaskService distributedTaskService,
                                                           TaskMapper taskMapper,
                                                           WorkerManager workerManager,
                                                           CommonSettings commonSettings) {
        return new DistributedTaskTestUtilImpl(
            taskRepository,
            dltRepository,
            distributedTaskService,
            taskMapper,
            workerManager,
            commonSettings
        );
    }
}

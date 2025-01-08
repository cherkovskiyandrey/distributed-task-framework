package com.distributed_task_framework.service.impl.workers;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.service.TaskSerializer;
import io.micrometer.core.instrument.Tag;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.DltRepository;
import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.impl.CronService;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;

import java.time.Clock;
import java.util.List;


@Slf4j
@FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
public class LocalExactlyOnceWorker extends LocalAtLeastOnceWorker implements TaskWorker {
    private static final List<Tag> COMMON_TAGS = List.of(Tag.of("name", "exactlyOnce"));

    public LocalExactlyOnceWorker(ClusterProvider clusterProvider,
                                  WorkerContextManager workerContextManager,
                                  PlatformTransactionManager transactionManager,
                                  InternalTaskCommandService internalTaskCommandService,
                                  TaskRepository taskRepository,
                                  RemoteCommandRepository remoteCommandRepository,
                                  DltRepository dltRepository,
                                  TaskSerializer taskSerializer,
                                  CronService cronService,
                                  TaskMapper taskMapper,
                                  CommonSettings commonSettings,
                                  TaskLinkManager taskLinkManager,
                                  MetricHelper metricHelper,
                                  Clock clock) {
        super(
                clusterProvider,
                workerContextManager,
                transactionManager,
                internalTaskCommandService,
                taskRepository,
                remoteCommandRepository,
                dltRepository,
                taskSerializer,
                cronService,
                taskMapper,
                commonSettings,
                taskLinkManager,
                metricHelper,
                clock
        );
    }

    @Override
    protected List<Tag> getCommonTags() {
        return COMMON_TAGS;
    }

    @Override
    public boolean isApplicable(TaskEntity taskEntity, TaskSettings taskParameters) {
        return TaskSettings.ExecutionGuarantees.EXACTLY_ONCE.equals(taskParameters.getExecutionGuarantees());
    }

    @Override
    protected <T, U> void runInternal(final TaskEntity taskEntity, RegisteredTask<T> registeredTask) {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transactionTemplate.executeWithoutResult(status -> super.runInternal(taskEntity, registeredTask));
    }
}

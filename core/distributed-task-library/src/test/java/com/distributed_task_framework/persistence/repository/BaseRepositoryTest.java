package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.internal.WorkerManager;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public class BaseRepositoryTest extends BaseSpringIntegrationTest {
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    @Qualifier("taskExtendedRepositoryImpl")
    TaskExtendedRepository taskExtendedRepository;
    @Autowired
    TaskPopulateAndVerify taskPopulateAndVerify;

    protected Set<Partition> toPartitions(List<TaskPopulateAndVerify.PopulationSpec> knownPopulationSpecs) {
        return knownPopulationSpecs.stream()
                .flatMap(populationSpec -> populationSpec.getNameOfTasks().stream()
                        .map(taskName -> Partition.builder()
                                .affinityGroup(populationSpec.getAffinityGroup())
                                .taskName(taskName)
                                .build()
                        )
                )
                .collect(Collectors.toSet());

    }

    protected void verifyTaskIsCanceled(TaskId taskId) {
        assertThat(taskRepository.find(taskId.getId()))
            .isPresent()
            .get()
            .matches(task -> Boolean.TRUE.equals(task.isCanceled()), "canceled");
    }

    protected void verifyTaskExecutionDate(TaskId taskId, Duration duration) {
        assertThat(taskRepository.find(taskId.getId()))
            .isPresent()
            .get()
            .matches(
                task -> LocalDateTime.now(clock).plus(duration).equals(task.getExecutionDateUtc()),
                "execution date"
            );
    }
}

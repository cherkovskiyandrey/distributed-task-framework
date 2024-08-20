package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.PartitionMapper;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.persistence.repository.PartitionRepository;
import com.distributed_task_framework.persistence.repository.PartitionTrackerRepository;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.settings.CommonSettings;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class PartitionTrackerImpl implements PartitionTracker {
    PlatformTransactionManager transactionManager;
    PartitionTrackerRepository partitionTrackerRepository;
    PartitionRepository partitionRepository;
    PartitionMapper partitionMapper;
    CommonSettings commonSettings;
    Clock clock;

    @Override
    public void reinit() {
        var partitionEntities = partitionTrackerRepository.activePartitions();
        var entities = partitionMapper.asEntities(partitionEntities, currentTimeWindow());
        partitionRepository.saveOrUpdateBatch(entities);
        log.info("reinit(): partitionEntities=[{}]", partitionEntities);
    }

    @Override
    public void track(Partition partition) {
        track(Set.of(partition));
    }

    @Override
    public void track(Set<Partition> partitions) {
        var partitionEntities = partitionMapper.asEntities(partitions, currentTimeWindow());
        var existedPartitionEntities = partitionRepository.filterExisted(partitionEntities);

        var toAdd = Sets.newHashSet(Sets.difference(
            Sets.newHashSet(partitionEntities),
            Sets.newHashSet(existedPartitionEntities))
        );
        if (!toAdd.isEmpty()) {
            partitionRepository.saveOrUpdateBatch(toAdd);
            log.info("track(): toAdd=[{}]", toAdd);
        }
    }

    @Override
    public Set<Partition> getAll() {
        return partitionMapper.fromEntities(partitionRepository.findAll());
    }

    @Override
    public void gcIfNecessary() {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        transactionTemplate.executeWithoutResult(status -> {
            var previousPartitionEntities = partitionRepository.findAllBeforeOrIn(currentTimeWindow() - 2);
            if (previousPartitionEntities.isEmpty()) {
                return;
            }

            var previousPartitions = partitionMapper.fromEntities(previousPartitionEntities);
            var movedPartitions = partitionTrackerRepository.filterInReadyVirtualQueue(previousPartitions);
            var removedPartitions = Sets.newHashSet(Sets.difference(previousPartitions, movedPartitions));
            if (!removedPartitions.isEmpty()) {
                log.info("gcIfNecessary(): removedPartitions=[{}]", removedPartitions);
            }
            partitionRepository.deleteBatch(previousPartitionEntities);
            track(movedPartitions);
        });
    }

    @Override
    public void compactIfNecessary() {
        partitionRepository.compactInTimeWindow(currentTimeWindow());
    }

    private Long currentTimeWindow() {
        var secondsInTimeWindow = commonSettings.getPlannerSettings().getPartitionTrackingTimeWindow().getSeconds();
        return Instant.now(clock).getEpochSecond() / secondsInTimeWindow;
    }
}

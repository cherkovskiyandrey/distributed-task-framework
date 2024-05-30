package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.model.AggregatedTaskStat;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

@Repository
public interface TaskStatRepository {

    List<AggregatedTaskStat> getAggregatedTaskStat(Set<String> knownTaskNames);
}

package com.distributed_task_framework.mapper;

import com.distributed_task_framework.model.Partition;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import com.distributed_task_framework.persistence.entity.PartitionEntity;

import java.util.Collection;
import java.util.Set;

@Mapper(
        unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface PartitionMapper {

    PartitionEntity asEntity(Partition partition, long timeBucket);

    default Collection<PartitionEntity> asEntities(Collection<Partition> partitions,
                                                   long timeBucket) {
        return partitions.stream()
                .map(partition -> asEntity(partition, timeBucket))
                .toList();
    }

    Partition fromEntity(PartitionEntity entity);

    Set<Partition> fromEntities(Iterable<PartitionEntity> entities);
}

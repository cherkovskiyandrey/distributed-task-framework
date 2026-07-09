package com.distributed_task_framework.utils;

import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import lombok.experimental.UtilityClass;

import java.util.Collection;
import java.util.Comparator;
import java.util.UUID;

@UtilityClass
public class PgComparatorUtils {
    //To sort in the same way as PG does for all entities: ORDER BY id
    private static final Comparator<UUID> NULL_SAFE_UUID_STRING_COMPARATOR = Comparator.comparing(id -> id == null ? "00000000-0000-0000-0000-000000000000" : id.toString());
    private static final Comparator<ShortTaskEntity> SHORT_TASK_ID_COMPARATOR = Comparator.comparing(ShortTaskEntity::getId, NULL_SAFE_UUID_STRING_COMPARATOR);
    private static final Comparator<TaskEntity> TASK_ID_COMPARATOR = Comparator.comparing(TaskEntity::getId, NULL_SAFE_UUID_STRING_COMPARATOR);

    public Collection<UUID> sortTaskIds(Collection<UUID> original) {
        return original.stream().sorted(NULL_SAFE_UUID_STRING_COMPARATOR).toList();
    }

    public Collection<ShortTaskEntity> sortShortTaskEntities(Collection<ShortTaskEntity> original) {
        return original.stream().sorted(SHORT_TASK_ID_COMPARATOR).toList();
    }

    public Collection<TaskEntity> sortTaskEntities(Collection<TaskEntity> original) {
        return original.stream().sorted(TASK_ID_COMPARATOR).toList();
    }
}

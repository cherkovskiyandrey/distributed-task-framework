package com.distributed_task_framework.utils;

import com.distributed_task_framework.persistence.entity.IdVersionEntity;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import lombok.experimental.UtilityClass;

import java.util.Comparator;
import java.util.UUID;

@UtilityClass
public class ComparatorUtils {
    // To sort in the same way as PG does.
    public static final Comparator<UUID> NULL_SAFE_UUID_STRING_COMPARATOR = Comparator.comparing(id -> id == null ? "00000000-0000-0000-0000-000000000000" : id.toString());
    public static final Comparator<ShortTaskEntity> SHORT_TASK_ID_COMPARATOR = Comparator.comparing(ShortTaskEntity::getId, NULL_SAFE_UUID_STRING_COMPARATOR);
    public static final Comparator<TaskEntity> TASK_ID_COMPARATOR = Comparator.comparing(TaskEntity::getId, NULL_SAFE_UUID_STRING_COMPARATOR);
    public static final Comparator<IdVersionEntity> ID_VERSION_ENTITY_COMPARATOR = Comparator.comparing(IdVersionEntity::getId, NULL_SAFE_UUID_STRING_COMPARATOR);
}

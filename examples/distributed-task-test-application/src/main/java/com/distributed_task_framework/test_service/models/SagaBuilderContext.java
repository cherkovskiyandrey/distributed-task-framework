package com.distributed_task_framework.test_service.models;

import com.distributed_task_framework.model.TaskDef;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

@Value
@Builder(toBuilder = true)
public class SagaBuilderContext {
    public static final SagaBuilderContext EMPTY_CONTEXT = SagaBuilderContext.builder().build();

    @Nullable
    String affinityGroup;
    @Nullable
    String affinity;
    @Nullable
    TaskDef<SagaContext> nextOperationTaskDef;

    public boolean hasAffinity() {
        return StringUtils.isNotBlank(affinityGroup)
                && StringUtils.isNotBlank(affinity);
    }
}

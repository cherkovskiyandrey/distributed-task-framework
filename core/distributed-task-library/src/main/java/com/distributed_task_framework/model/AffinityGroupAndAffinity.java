package com.distributed_task_framework.model;

import lombok.experimental.FieldNameConstants;

@FieldNameConstants
public record AffinityGroupAndAffinity(String affinityGroup, String affinity) {
}

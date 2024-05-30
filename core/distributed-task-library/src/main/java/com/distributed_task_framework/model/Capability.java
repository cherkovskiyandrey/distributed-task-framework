package com.distributed_task_framework.model;

import lombok.Value;

@Value(staticConstructor = "of")
public class Capability {
    String name;
}

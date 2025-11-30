package com.distributed_task_framework.task;

import com.distributed_task_framework.model.TypeDef;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;

@SuperBuilder(builderMethodName = "buildStateful")
@Getter
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TestStatefulTaskModel<T, U> extends TestTaskModel<T> {
    TypeDef<U> stateDef;
}

package com.distributed_task_framework.local_commands;

import jakarta.annotation.Nullable;

public interface BatcheableLocalCommand<BATCH_TYPE extends LocalCommand> extends LocalCommand {

    Class<BATCH_TYPE> batchClass();

    BATCH_TYPE addToBatch(@Nullable BATCH_TYPE batch);
}

package com.distributed_task_framework.service.internal;

import javax.annotation.Nullable;

public interface BatcheableLocalCommand<BATCH_TYPE extends LocalCommand> extends LocalCommand {

    Class<BATCH_TYPE> batchClass();

    BATCH_TYPE addToBatch(@Nullable BATCH_TYPE batch);
}

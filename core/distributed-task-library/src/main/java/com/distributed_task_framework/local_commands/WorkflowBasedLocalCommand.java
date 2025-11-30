package com.distributed_task_framework.local_commands;

import java.util.Collection;
import java.util.UUID;

public interface WorkflowBasedLocalCommand extends LocalCommand {
    Collection<UUID> workflows();
}

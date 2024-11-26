package com.distributed_task_framework.test.autoconfigure.tasks;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test.autoconfigure.Signaller;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class InfinitiveWorkflowGenerationExampleTask implements Task<Integer> {
    public static final TaskDef<Integer> TASK_DEF = TaskDef.privateTaskDef(
        "infinitive-workflow-generation",
        Integer.class
    );

    DistributedTaskService distributedTaskService;
    Signaller signaller;

    @Override
    public TaskDef<Integer> getDef() {
        return TASK_DEF;
    }

    @Override
    public void execute(ExecutionContext<Integer> executionContext) throws Exception {
        if (executionContext.getInputMessageOrThrow() == 1) {
            signaller.getCyclicBarrierRef().get().await();
        }
        log.info("Current invoke is #{}", executionContext.getInputMessageOrThrow());
        distributedTaskService.schedule(
            InfinitiveWorkflowGenerationExampleTask.TASK_DEF,
            executionContext.withNewMessage(executionContext.getInputMessageOrThrow() + 1)
        );
    }
}

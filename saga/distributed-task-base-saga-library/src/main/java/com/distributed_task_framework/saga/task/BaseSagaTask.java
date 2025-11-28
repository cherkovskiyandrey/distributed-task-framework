package com.distributed_task_framework.saga.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.saga.models.SagaAction;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.service.DistributedTaskService;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PROTECTED)
public abstract class BaseSagaTask {
    SagaResolver sagaResolver;
    SagaManager sagaManager;
    DistributedTaskService distributedTaskService;

    /**
     * Schedule next saga method as a task.
     *
     * Take into account case when next saga action (task) is unknown on current node.
     * But can be known on other node in cluster.
     * This case can be during rolling out new version of node with new saga action:
     * <ol>
     *    <li>sqs</li>
     * </ol>
     * <table>
     *     <tr>
     *         <th>Current node register</th>
     *         <th>New node register</th>
     *         <th>Execution chain</th>
     *     </tr>
     *     <tr>
     *         <td>sa1 -> sa2 -> sa3</td>
     *         <td>sa1 -> sa2 -> sa3_v2[sa3 - old version for backward compatibility]</td>
     *         <td>new-node: sa1 -> current_node: sa2 -(*)-> new-node: sa3_v2</td>
     *     </tr>
     *     <tr>
     *         <td>sa1/rsa1 -> sa2 -> sa3</td>
     *         <td>sa1/rsa1_v2[rsa1 - old version for backward compatibility] -> sa2 -> sa3</td>
     *         <td>new-node: sa1 -> current_node: sa2(need rollback) -(*)-> new-node: rsa1_v2</td>
     *     </tr>
     * </table>
     *
     * @param executionContext
     * @param sagaPipeline
     * @param sagaMethodProvider
     * @throws Exception
     */
    protected void scheduleNextOrComplete(ExecutionContext<SagaPipeline> executionContext,
                                          SagaPipeline sagaPipeline,
                                          Function<SagaAction, String> sagaMethodProvider) throws Exception {
        if (sagaPipeline.hasNext()) {
            sagaPipeline.moveToNext();
            var nextSagaContext = sagaPipeline.getCurrentAction();
            var sagaMethodName = sagaMethodProvider.apply(nextSagaContext);
            var nextTaskDefOpt = sagaResolver.resolveByTaskNameIfExists(sagaMethodName);
            if (nextTaskDefOpt.isPresent()) {
                distributedTaskService.schedule(
                    nextTaskDefOpt.get(),
                    executionContext.withNewMessage(sagaPipeline)
                );
            } else {
                distributedTaskService.scheduleUnsafe(
                    sagaResolver.resolveByTaskNameInCluster(sagaMethodName),
                    executionContext.withNewMessage(sagaPipeline)
                );
            }
            sagaManager.trackIfExists(sagaPipeline);
        } else {
            sagaManager.completeIfExists(sagaPipeline.getSagaId());
        }
    }
}

package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.WorkerContext;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import com.distributed_task_framework.service.internal.TaskCommandWithDetectorService;
import com.distributed_task_framework.service.internal.WorkerContextManager;

import java.util.Optional;

@Slf4j
@FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
@AllArgsConstructor
public abstract class AbstractTaskCommandWithDetectorService implements TaskCommandWithDetectorService {
    WorkerContextManager workerContextManager;
    PlatformTransactionManager transactionManager;

    protected void executeTxAware(RunnableWithException action, boolean isImmediately) throws Exception {
        executeTxAware(
                () -> {
                    action.execute();
                    return null;
                },
                isImmediately
        );
    }

    protected <U> U executeTxAware(SupplierWithException<U> action, boolean isImmediately) throws Exception {
        Optional<WorkerContext> currentContext = workerContextManager.getCurrentContext();
        if (currentContext.isPresent() &&
                isImmediately &&
                currentContext.get().alreadyInTransaction()) {
            TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
            transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
            transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            return transactionTemplate.execute(status -> {
                try {
                    return action.get();
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            });
        }

        return action.get();
    }

    protected interface SupplierWithException<U> {
        U get() throws Exception;
    }

    protected interface RunnableWithException {
        void execute() throws Exception;
    }
}

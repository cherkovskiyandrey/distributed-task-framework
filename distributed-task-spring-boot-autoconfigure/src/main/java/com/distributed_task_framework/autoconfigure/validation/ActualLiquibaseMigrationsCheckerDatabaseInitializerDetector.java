package com.distributed_task_framework.autoconfigure.validation;

import org.springframework.boot.sql.init.dependency.AbstractBeansOfTypeDatabaseInitializerDetector;

import java.util.Collections;
import java.util.Set;

public class ActualLiquibaseMigrationsCheckerDatabaseInitializerDetector extends AbstractBeansOfTypeDatabaseInitializerDetector {

    @Override
    protected Set<Class<?>> getDatabaseInitializerBeanTypes() {
        return Collections.singleton(ActualLiquibaseMigrationsChecker.class);
    }
}

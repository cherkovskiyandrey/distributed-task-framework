package com.distributed_task_framework.autoconfigure.validation;

import liquibase.integration.spring.SpringLiquibase;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

public class CheckActualLiquibaseMigrationsListener implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        var springLiquibase = event.getApplicationContext().getBean(SpringLiquibase.class);
        var checker = event.getApplicationContext().getBean(ActualLiquibaseMigrationsChecker.class);
        checker.check(springLiquibase);
    }
}

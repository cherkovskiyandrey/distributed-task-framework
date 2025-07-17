package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.validation.CheckActualLiquibaseMigrationsListener;
import liquibase.integration.spring.SpringLiquibase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;

@Slf4j
@AutoConfiguration(after = {LiquibaseAutoConfiguration.class})
@ConditionalOnClass(JdbcTemplate.class)
@ConditionalOnBean(SpringLiquibase.class)
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "true")
public class DistributedTaskLiquibaseAutoConfiguration {

    @Bean
    @Conditional(CheckMigrationsCondition.class)
    public CheckActualLiquibaseMigrationsListener actualLiquibaseMigrationsChecker() {
        return new CheckActualLiquibaseMigrationsListener();
    }

    static class CheckMigrationsCondition implements Condition {
        private static final String CHECK_MIGRATIONS_PROPERTY = "distributed-task.liquibase.check-migrations";

        @Override
        public boolean matches(ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
            var checkMigrations = context.getEnvironment()
                .getProperty(CHECK_MIGRATIONS_PROPERTY, Boolean.class);
            if (Boolean.FALSE.equals(checkMigrations)) {
                log.warn(
                    "Check database migrations is disabled due to property '{}' is not set. " +
                        "It's highly recommended to check the migrations for proper DTF work",
                    CHECK_MIGRATIONS_PROPERTY
                );
                return false;
            }
            return true;
        }
    }
}

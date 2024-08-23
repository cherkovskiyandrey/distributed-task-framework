package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.annotation.DtfDataSource;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class DtfDataSourceCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        final ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
        if (beanFactory == null)
            return true;

        final String[] beanNamesForAnnotation = beanFactory.getBeanNamesForAnnotation(DtfDataSource.class);
        return beanNamesForAnnotation.length == 0;
    }
}

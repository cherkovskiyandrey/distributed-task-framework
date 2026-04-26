package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.repository.Repository;

import java.io.Serializable;
import java.util.Arrays;

public class DtfJdbcRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
    extends JdbcRepositoryFactoryBean<T, S, ID> {
    private DtfJdbcInfrastructure infrastructure;

    public DtfJdbcRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    @Autowired
    public void setInfrastructure(DtfJdbcInfrastructure infrastructure) {
        this.infrastructure = infrastructure;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        Arrays.stream(((ListableBeanFactory) beanFactory).getBeanNamesForType(infrastructure.getPlatformTransactionManager().getClass()))
            .filter(txName -> beanFactory.getBean(txName) == infrastructure.getPlatformTransactionManager())
            .findFirst()
            .ifPresent(this::setTransactionManager);
        super.setBeanFactory(beanFactory);
    }

    @Override
    public void afterPropertiesSet() {
        setDataAccessStrategy(infrastructure.getDataAccessStrategy());
        setMappingContext(infrastructure.getJdbcMappingContext());
        setConverter(infrastructure.getConverter());
        setDialect(infrastructure.getDialect());
        setJdbcOperations(infrastructure.getNamedParameterJdbcOperations());
        super.afterPropertiesSet();
    }
}

package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.repository.Repository;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.Serializable;
import java.util.Arrays;

public class DtfJdbcRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
    extends JdbcRepositoryFactoryBean<T, S, ID> {
    private DtfJdbcInfrastructure infrastructure;
    private ConfigurableListableBeanFactory beanFactory;

    public DtfJdbcRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    @Autowired
    public void setInfrastructure(DtfJdbcInfrastructure infrastructure) {
        this.infrastructure = infrastructure;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
        super.setBeanFactory(beanFactory);
    }

    @Override
    public void afterPropertiesSet() {
        detectAndSetTransactionManager();
        setDataAccessStrategy(infrastructure.getDataAccessStrategy());
        setMappingContext(infrastructure.getJdbcMappingContext());
        setConverter(infrastructure.getConverter());
        setDialect(infrastructure.getDialect());
        setJdbcOperations(infrastructure.getNamedParameterJdbcOperations());
        super.afterPropertiesSet();
    }

    private void detectAndSetTransactionManager() {
        var dtfPlatformTransactionManager = infrastructure.getPlatformTransactionManager();
        var allTxManagers = Arrays.stream(beanFactory.getBeanNamesForType(
                PlatformTransactionManager.class,
                false,
                false
            )
        ).toList();

        var txNameOpt = allTxManagers.stream()
            .filter(txName -> beanFactory.containsSingleton(txName))
            .filter(txName -> beanFactory.getBean(txName) == dtfPlatformTransactionManager)
            .findFirst();

        if (txNameOpt.isEmpty()) {
            throw new BeanInitializationException(("Can't detect bean name of PlatformTransactionManager [%s] " +
                " from DtfJdbcInfrastructure among candidates %s. It must be a bean of the application context.")
                .formatted(dtfPlatformTransactionManager, allTxManagers)
            );
        }
        setTransactionManager(txNameOpt.get());
    }
}

package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategyFactory;
import org.springframework.data.jdbc.core.convert.DefaultJdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.InsertStrategyFactory;
import org.springframework.data.jdbc.core.convert.JdbcArrayColumns;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.jdbc.core.convert.SqlParametersFactory;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.repository.config.AbstractJdbcConfiguration;
import org.springframework.data.jdbc.repository.config.DialectResolver;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.DefaultNamingStrategy;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Optional;

import static org.mockito.Mockito.when;

@Slf4j
@TestConfiguration
@ActiveProfiles("test")
public class DataSourceConfiguration { //extends AbstractJdbcConfiguration {
    @Primary
    @Bean
    public DataSource primaryDataSource() throws SQLException {
        final DataSource dataSource = mockDataSource("primary");
        when(dataSource.toString()).thenReturn("primary");
        return dataSource;
    }

    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    @Primary
    public NamedParameterJdbcOperations namedParameterJdbcOperations(DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    @Qualifier
    public DataSource dtfDataSource() throws SQLException {
        final DataSource dataSource = mockDataSource("dtf");
        when(dataSource.toString()).thenReturn("dtf");
        return dataSource;
    }

    @Bean
    @Qualifier
    public PlatformTransactionManager dtfTransactionManager(@Qualifier DataSource dtfDataSource) {
        return new DataSourceTransactionManager(dtfDataSource);
    }

    @Bean
    @Qualifier
    public NamedParameterJdbcOperations dtfNamedParameterJdbcOperations(@Qualifier DataSource dtfDataSource) {
        return new NamedParameterJdbcTemplate(dtfDataSource);
    }


//    @Bean("dtfJdbcDialect")
//    @Qualifier("dtfJdbcDialect")
//    @Override
//    public Dialect jdbcDialect(@Qualifier NamedParameterJdbcOperations dtfNamedParameterJdbcOperations) {
//        return super.jdbcDialect(dtfNamedParameterJdbcOperations);
//    }
    @Bean
    @Qualifier
    public Dialect dtfJdbcDialect(@Qualifier NamedParameterJdbcOperations dtfNamedParameterJdbcOperations) {
        return DialectResolver.getDialect(dtfNamedParameterJdbcOperations.getJdbcOperations());
    }


//    @Bean("dtfJdbcMappingContext")
//    @Qualifier("dtfJdbcMappingContext")
//    @Override
//    public JdbcMappingContext jdbcMappingContext(Optional<NamingStrategy> namingStrategy,
//                                                 JdbcCustomConversions customConversions,
//                                                 RelationalManagedTypes jdbcManagedTypes) {
//        return super.jdbcMappingContext(namingStrategy, customConversions, jdbcManagedTypes);
//    }
    @Bean
    @Qualifier
    public JdbcMappingContext dtfJdbcMappingContext(Optional<NamingStrategy> namingStrategy,
                                                    JdbcCustomConversions dtfCustomConversions,
                                                    RelationalManagedTypes jdbcManagedTypes) {
        JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy.orElse(DefaultNamingStrategy.INSTANCE));
        mappingContext.setSimpleTypeHolder(dtfCustomConversions.getSimpleTypeHolder());
        mappingContext.setManagedTypes(jdbcManagedTypes);

        return mappingContext;
    }

//    @Bean("dtfJdbcConverter")
//    @Qualifier("dtfJdbcConverter")
//    @Override
//    public JdbcConverter jdbcConverter(@Qualifier JdbcMappingContext jdbcMappingContext,
//                                       @Qualifier NamedParameterJdbcOperations dtfNamedParameterJdbcOperations,
//                                       @Lazy RelationResolver relationResolver,
//                                       JdbcCustomConversions conversions,
//                                       @Qualifier Dialect dtfJdbcDialect) {
//        return super.jdbcConverter(
//            jdbcMappingContext,
//            dtfNamedParameterJdbcOperations,
//            relationResolver,
//            conversions,
//            dtfJdbcDialect
//        );
//    }
    @Bean
    @Qualifier
    public JdbcConverter dtfJdbcConverter(@Qualifier JdbcMappingContext dtfJdbcMappingContext,
                                          @Qualifier NamedParameterJdbcOperations dtfNamedParameterJdbcOperations,
                                          @Lazy RelationResolver relationResolver,
                                          JdbcCustomConversions conversions,
                                          @Qualifier Dialect dtfDialect) {

        JdbcArrayColumns arrayColumns = dtfDialect instanceof JdbcDialect ? ((JdbcDialect) dtfDialect).getArraySupport()
            : JdbcArrayColumns.DefaultSupport.INSTANCE;
        DefaultJdbcTypeFactory jdbcTypeFactory = new DefaultJdbcTypeFactory(dtfNamedParameterJdbcOperations.getJdbcOperations(), arrayColumns);

        return new MappingJdbcConverter(dtfJdbcMappingContext, relationResolver, conversions, jdbcTypeFactory);
    }


//    @Bean("dtfDataAccessStrategy")
//    @Qualifier("dtfDataAccessStrategy")
//    @Override
//    public DataAccessStrategy dataAccessStrategyBean(@Qualifier NamedParameterJdbcOperations dtfNamedParameterJdbcOperations,
//                                                     @Qualifier JdbcConverter dtfJdbcConverter,
//                                                     @Qualifier JdbcMappingContext jdbcMappingContext,
//                                                     @Qualifier Dialect dtfJdbcDialect) {
//        return super.dataAccessStrategyBean(
//            dtfNamedParameterJdbcOperations,
//            dtfJdbcConverter,
//            jdbcMappingContext,
//            dtfJdbcDialect
//        );
//    }
    @Bean
    @Qualifier
    public DataAccessStrategy dtfDataAccessStrategy(
        @Qualifier NamedParameterJdbcOperations dtfNamedParameterJdbcOperations,
        @Qualifier Dialect dtfJdbcDialect,
        @Qualifier JdbcConverter dtfJdbcConverter,
        @Qualifier JdbcMappingContext dtfJdbcMappingContext) {
        SqlGeneratorSource sqlGeneratorSource = new SqlGeneratorSource(dtfJdbcMappingContext, dtfJdbcConverter, dtfJdbcDialect);
        DataAccessStrategyFactory factory = new DataAccessStrategyFactory(sqlGeneratorSource, dtfJdbcConverter, dtfNamedParameterJdbcOperations,
            new SqlParametersFactory(dtfJdbcMappingContext, dtfJdbcConverter),
            new InsertStrategyFactory(dtfNamedParameterJdbcOperations, dtfJdbcDialect));

        return factory.create();
    }

    @Bean
    @ConditionalOnMissingBean(DtfJdbcInfrastructure.class)
    public DtfJdbcInfrastructure dtfJdbcInfrastructure(
        @Qualifier PlatformTransactionManager dtfTransactionManager,
        @Qualifier NamedParameterJdbcOperations dtfNamedParameterJdbcOperations,
        @Qualifier Dialect dtfJdbcDialect,
        @Qualifier DataAccessStrategy dtfDataAccessStrategy,
        @Qualifier JdbcConverter dtfJdbcConverter,
        @Qualifier JdbcMappingContext jdbcMappingContext
    ) {
        return new DtfJdbcInfrastructure(
            dtfTransactionManager,
            dtfNamedParameterJdbcOperations,
            dtfJdbcDialect,
            dtfDataAccessStrategy,
            jdbcMappingContext,
            dtfJdbcConverter
        );
    }

    private DataSource mockDataSource(String dsName) throws SQLException {
        final DataSource dataSource = Mockito.mock(DataSource.class);
        final Connection connection = Mockito.mock(Connection.class);
        final DatabaseMetaData metadata = Mockito.mock(DatabaseMetaData.class);

        when(dataSource.getConnection()).then(invocation -> {
                log.info(
                    "mockDataSource(): dsName=[{}], stack trace:",
                    dsName,
                    //System.identityHashCode(applicationContext),
                    new Exception("Debug stack trace")
                );
                return connection;
            }
        );
        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");

        return dataSource;
    }

}

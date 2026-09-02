package com.distributed_task_framework.utils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.transaction.PlatformTransactionManager;

@Getter
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class DtfJdbcInfrastructure {
    PlatformTransactionManager platformTransactionManager;
    NamedParameterJdbcOperations namedParameterJdbcOperations;
    Dialect dialect;
    DataAccessStrategy dataAccessStrategy;
    JdbcMappingContext jdbcMappingContext;
    JdbcConverter converter;
}

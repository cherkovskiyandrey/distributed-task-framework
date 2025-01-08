package com.distributed_task_framework.autoconfigure.annotation;

import org.springframework.beans.factory.annotation.Qualifier;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotate {@link javax.sql.DataSource} that DTF should use. If no annotated {@link javax.sql.DataSource} found, then primary would be used.
 * <pre>
 *     {@code
 *
 *      @Bean
 *      @DtfDataSource
 *      public DataSource dtfDataSource(@Qualifier("specialDataSource") DataSource specialDataSource) {
 *          return specialDataSource;
 *      }
 *     }
 * </pre>
 */
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Qualifier
public @interface DtfDataSource {
}

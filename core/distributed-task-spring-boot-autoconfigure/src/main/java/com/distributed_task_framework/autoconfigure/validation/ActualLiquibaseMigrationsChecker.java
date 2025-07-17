package com.distributed_task_framework.autoconfigure.validation;

import liquibase.integration.spring.SpringLiquibase;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SingleColumnRowMapper;

import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarFile;

@Slf4j
public class ActualLiquibaseMigrationsChecker {
    private static final String SELECT_FILENAMES_QUERY = "SELECT filename FROM %s";
    private static final String LIQUIBASE_SCRIPTS_PATH = "db/changelog/distributed-task-framework";

    public void check(SpringLiquibase springLiquibase) {
        Set<String> knownMigrationFileNames;
        Set<String> deployedMigrationFileNames;
        try {
            knownMigrationFileNames = knownMigrationFileNames(springLiquibase);
            log.debug("Known Liquibase migrations: {}", knownMigrationFileNames);

            deployedMigrationFileNames = deployedMigrationFileNames(springLiquibase);
            log.debug("Deployed Liquibase migrations: {}", deployedMigrationFileNames);

        } catch (Exception e) {
            log.warn("Unable to check liquibase migrations. Skipping...", e);
            return;
        }

        knownMigrationFileNames.removeAll(deployedMigrationFileNames);
        if (!knownMigrationFileNames.isEmpty()) {
            throw new IllegalStateException("Followed migrations are not deployed " + knownMigrationFileNames);
        }
    }

    private Set<String> knownMigrationFileNames(SpringLiquibase springLiquibase) {
        var fileNames = Optional.ofNullable(springLiquibase.getClass().getClassLoader())
            .map(classLoader -> classLoader.getResource(LIQUIBASE_SCRIPTS_PATH))
            .map(resource -> {
                var jarPath = parseJarFileName(resource);
                return findAllLiquibaseScriptNames(jarPath);
            })
            .orElse(Collections.emptySet());
        fileNames.removeIf(fileName -> fileName.endsWith("db.changelog-aggregator.yaml"));
        return fileNames;
    }

    private String parseJarFileName(URL resourceName) {
        var path = resourceName.getPath();
        System.out.println(path);
        // path looks like 'file:absolute-path-to-file.jar!',
        // so we need to take only 'absolute-path-to-file.jar'
        return path.substring(5, path.indexOf("!"));
    }

    @SneakyThrows
    private Set<String> findAllLiquibaseScriptNames(String jarPath) {
        var fileNames = new HashSet<String>();
        try (var jarFile = new JarFile(jarPath)) {
            var entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                var entry = entries.nextElement();
                var name = entry.getName();
                if (name.startsWith(LIQUIBASE_SCRIPTS_PATH) && (name.endsWith(".yaml") || name.endsWith(".yml"))) {
                    fileNames.add(name);
                }
            }
        }
        return fileNames;
    }

    private Set<String> deployedMigrationFileNames(SpringLiquibase springLiquibase) {
        var jdbcTemplate = new JdbcTemplate(springLiquibase.getDataSource());
        var fileNames = jdbcTemplate.query(
            SELECT_FILENAMES_QUERY.formatted(springLiquibase.getDatabaseChangeLogTable()),
            new SingleColumnRowMapper<>(String.class)
        );
        return new HashSet<>(fileNames);
    }
}

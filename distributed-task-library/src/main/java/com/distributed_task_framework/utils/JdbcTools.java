package com.distributed_task_framework.utils;

import lombok.experimental.UtilityClass;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@UtilityClass
public class JdbcTools {

    @Nullable
    public <T> String asNullableString(T t) {
        if (t == null) {
            return null;
        }
        return t.toString();
    }

    public String[] UUIDsToStringArray(Collection<UUID> inputUUIDs) {
        return inputUUIDs.stream()
                .map(UUID::toString)
                .toList()
                .toArray(new String[0]);
    }

    public String[] toArray(Collection<String> strings) {
        return strings.toArray(new String[0]);
    }

    public Long[] toLongArray(Collection<Long> longs) {
        return longs.toArray(new Long[0]);
    }

    public <T extends Enum<T>> String[] toEnumArray(EnumSet<T> enumValues) {
        return enumValues.stream()
                .map(Enum::toString)
                .toArray(String[]::new);
    }

    @Nullable
    public <T extends Enum<T>> String asString(@Nullable Enum<T> value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    public <T> List<T> filterAffected(List<T> entities, int[] results) {
        return filter(entities, results, i -> i > 0);
    }

    public <T> List<T> filterNotAffected(List<T> entities, int[] results) {
        return filter(entities, results, i -> i == 0);
    }

    private <T> List<T> filter(List<T> entities, int[] results, Predicate<Integer> predicate) {
        List<T> affected = new ArrayList<>();
        for (int i = 0; i < entities.size(); i++) {
            if (predicate.test(results[i])) {
                affected.add(entities.get(i));
            }
        }
        return affected;
    }

    public <T> String valueOrNullExpression(@Nullable T t) {
        return t == null ? " IS NULL" : " = '" + t + "'";
    }

    public static String stringValueOrNullObject(String string) {
        return string == null ? "null" : "'" + string + "'";
    }

    public static final String COMMON_AGGREGATED_SELECT_TEMPLATE = """
            SELECT * FROM {TABLE_NAME}
            """;

    public static String buildCommonAggregatedTable(int size, String table_prefix) {
        return IntStream.range(0, size)
                .mapToObj(i -> COMMON_AGGREGATED_SELECT_TEMPLATE
                        .replace("{TABLE_NAME}", table_prefix + i)
                )
                .collect(Collectors.joining(" UNION ALL "));
    }
}

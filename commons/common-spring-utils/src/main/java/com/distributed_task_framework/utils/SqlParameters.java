package com.distributed_task_framework.utils;

import lombok.experimental.UtilityClass;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Function;

@UtilityClass
public class SqlParameters {

    public static MapSqlParameterSource of(String n1, @Nullable Object o1, int st1) {
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(n1, o1, st1);
        return mapSqlParameterSource;
    }

    public static MapSqlParameterSource of(
        String n1, @Nullable Object o1, int st1,
        String n2, @Nullable Object o2, int st2) {
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(n1, o1, st1);
        mapSqlParameterSource.addValue(n2, o2, st2);
        return mapSqlParameterSource;
    }

    public static MapSqlParameterSource of(
        String n1, @Nullable Object o1, int st1,
        String n2, @Nullable Object o2, int st2,
        String n3, @Nullable Object o3, int st3) {
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(n1, o1, st1);
        mapSqlParameterSource.addValue(n2, o2, st2);
        mapSqlParameterSource.addValue(n3, o3, st3);
        return mapSqlParameterSource;
    }

    public static MapSqlParameterSource of(
        String n1, @Nullable Object o1, int st1,
        String n2, @Nullable Object o2, int st2,
        String n3, @Nullable Object o3, int st3,
        String n4, @Nullable Object o4, int st4) {
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(n1, o1, st1);
        mapSqlParameterSource.addValue(n2, o2, st2);
        mapSqlParameterSource.addValue(n3, o3, st3);
        mapSqlParameterSource.addValue(n4, o4, st4);
        return mapSqlParameterSource;
    }

    public static MapSqlParameterSource of(
        String n1, @Nullable Object o1, int st1,
        String n2, @Nullable Object o2, int st2,
        String n3, @Nullable Object o3, int st3,
        String n4, @Nullable Object o4, int st4,
        String n5, @Nullable Object o5, int st5) {
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(n1, o1, st1);
        mapSqlParameterSource.addValue(n2, o2, st2);
        mapSqlParameterSource.addValue(n3, o3, st3);
        mapSqlParameterSource.addValue(n4, o4, st4);
        mapSqlParameterSource.addValue(n5, o5, st5);
        return mapSqlParameterSource;
    }

    public static MapSqlParameterSource of(
        String n1, @Nullable Object o1, int st1,
        String n2, @Nullable Object o2, int st2,
        String n3, @Nullable Object o3, int st3,
        String n4, @Nullable Object o4, int st4,
        String n5, @Nullable Object o5, int st5,
        String n6, @Nullable Object o6, int st6) {
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(n1, o1, st1);
        mapSqlParameterSource.addValue(n2, o2, st2);
        mapSqlParameterSource.addValue(n3, o3, st3);
        mapSqlParameterSource.addValue(n4, o4, st4);
        mapSqlParameterSource.addValue(n5, o5, st5);
        mapSqlParameterSource.addValue(n6, o6, st6);
        return mapSqlParameterSource;
    }

    public static MapSqlParameterSource of(
        String n1, @Nullable Object o1, int st1,
        String n2, @Nullable Object o2, int st2,
        String n3, @Nullable Object o3, int st3,
        String n4, @Nullable Object o4, int st4,
        String n5, @Nullable Object o5, int st5,
        String n6, @Nullable Object o6, int st6,
        String n7, @Nullable Object o7, int st7) {
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(n1, o1, st1);
        mapSqlParameterSource.addValue(n2, o2, st2);
        mapSqlParameterSource.addValue(n3, o3, st3);
        mapSqlParameterSource.addValue(n4, o4, st4);
        mapSqlParameterSource.addValue(n5, o5, st5);
        mapSqlParameterSource.addValue(n6, o6, st6);
        mapSqlParameterSource.addValue(n7, o7, st7);
        return mapSqlParameterSource;
    }

    public static <T> SqlParameterSource[] convert(Collection<T> entities,
                                                   Function<T, SqlParameterSource> convertor) {
        return entities.stream()
            .map(convertor)
            .toArray(SqlParameterSource[]::new);
    }
}

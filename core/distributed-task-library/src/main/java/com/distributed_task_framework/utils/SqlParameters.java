package com.distributed_task_framework.utils;

import lombok.experimental.UtilityClass;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import javax.annotation.Nullable;

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
}

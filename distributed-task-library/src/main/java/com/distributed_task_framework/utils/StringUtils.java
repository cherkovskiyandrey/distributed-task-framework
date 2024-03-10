package com.distributed_task_framework.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class StringUtils {

    public String requireNotBlank(String value, String name) {
        if (org.apache.commons.lang3.StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(name + " must not be empty string");
        }
        return value;
    }
}

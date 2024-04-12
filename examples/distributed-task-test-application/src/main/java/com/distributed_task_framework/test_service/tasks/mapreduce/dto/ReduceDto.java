package com.distributed_task_framework.test_service.tasks.mapreduce.dto;

import java.util.Map;

public record ReduceDto(Map<String, Integer> wordFreq) {
}

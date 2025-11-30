package com.distributed_task_framework.autoconfigure.mapper;

import com.distributed_task_framework.exception.TaskConfigurationException;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.NullValuePropertyMappingStrategy;

import java.util.List;
import java.util.Map;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface RangeDelayMapper {

    default ImmutableRangeMap<Integer, Integer> mapRangeDelayProperty(Map<Integer, Integer> rangeDelay) {
        List<Integer> orderedNumbers = rangeDelay.keySet().stream()
            .sorted()
            .toList();
        var rangeMapBuilder = ImmutableRangeMap.<Integer, Integer>builder();
        int lastNumber = -1;
        for (int number : orderedNumbers) {
            if (number <= lastNumber) {
                throw new TaskConfigurationException("Incorrect ordering of polling-delay");
            }
            rangeMapBuilder.put(Range.openClosed(lastNumber, number), rangeDelay.get(number));
            lastNumber = number;
        }
        return rangeMapBuilder.build();
    }
}

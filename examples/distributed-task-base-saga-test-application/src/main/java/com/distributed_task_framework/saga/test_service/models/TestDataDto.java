package com.distributed_task_framework.saga.test_service.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.DurationDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.DurationSerializer;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.NotEmpty;

import java.time.Duration;

@Validated
@Value
@Builder
@Jacksonized
public class TestDataDto {
    @NotEmpty
    Long id;
    @NotEmpty
    Long version;

    @NotEmpty
    String remoteServiceOneId;
    @NotEmpty
    String remoteOneData;

    @NotEmpty
    String remoteServiceTwoId;
    @NotEmpty
    String remoteTwoData;

    Integer throwExceptionOnLevel;

    Integer levelToDelay;
    @Schema(type = "string", format = "duration")
    Duration syntheticDelayOnEachLevel;
}

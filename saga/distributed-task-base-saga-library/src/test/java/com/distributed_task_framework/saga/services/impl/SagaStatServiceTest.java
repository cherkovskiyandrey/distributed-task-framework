package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.models.CreateSagaRequest;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.persistence.entities.AggregatedSagaStat;
import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import io.micrometer.core.instrument.Meter;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.distributed_task_framework.saga.persistence.entities.AggregatedSagaStat.State.ACTIVE;
import static com.distributed_task_framework.saga.persistence.entities.AggregatedSagaStat.State.COMPLETED;
import static com.distributed_task_framework.saga.persistence.entities.AggregatedSagaStat.State.COMPLETED_CLEANING;
import static com.distributed_task_framework.saga.persistence.entities.AggregatedSagaStat.State.COMPLETED_NOT_CLEANED;
import static com.distributed_task_framework.saga.persistence.entities.AggregatedSagaStat.State.EXPIRED;
import static com.distributed_task_framework.saga.persistence.entities.AggregatedSagaStat.State.EXPIRED_NOT_CLEANED;
import static org.mockito.Mockito.when;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class SagaStatServiceTest extends BaseSpringIntegrationTest {
    Map<AggregatedSagaStat.State, Function<SagaEntity, SagaEntity>> sagaEntityPatcherByState;

    @SneakyThrows
    @BeforeEach
    public void init() {
        super.init();
        when(plannerService.isActive()).thenReturn(false);
        sagaManager.enableHandleDeprecatedSagas(false); //turn off to prevent races with these tests
        waitFor(() -> !sagaManager.isHandleDeprecatedSagasEnabled()); //wait for turning off
        setFixedTime(1000);
        initSagaEntityPatcherByState();
    }

    @AfterEach
    public void revert() {
        sagaManager.enableHandleDeprecatedSagas(true); //return to back for other tests
    }

    @SneakyThrows
    @Test
    void shouldCalculateStatWhenActive() {
        //when
        when(plannerService.isActive()).thenReturn(true);
        populateSagaEntities(ACTIVE, Map.of(0, 5, 1, 4, 2, 3, 3, 1)); //5+4+3+1=13
        populateSagaEntities(COMPLETED, Map.of(0, 4, 1, 3, 2, 2, 3, 1)); //4+3+2+1=10
        populateSagaEntities(COMPLETED_CLEANING, Map.of(0, 5, 1, 4, 2, 3, 3, 2)); //5+4+3+2=14
        populateSagaEntities(COMPLETED_NOT_CLEANED, Map.of(0, 6, 1, 5, 2, 4, 3, 3)); //6+5+4+3=18
        populateSagaEntities(EXPIRED, Map.of(0, 5, 1, 4, 2, 3, 3, 1)); //5+4+3+1=13
        populateSagaEntities(EXPIRED_NOT_CLEANED, Map.of(0, 4, 1, 3, 2, 2, 3, 1)); //4+3+2+1=10

        //do
        sagaStatService.calculateStat();

        //verify
        metricTestHelper.assertMetric(distributedTaskMetricHelper.buildName("saga", "state", "all"), 78);
        metricTestHelper.assertMetric(distributedTaskMetricHelper.buildName("saga", "state", "active"), 13);
        metricTestHelper.assertMetric(distributedTaskMetricHelper.buildName("saga", "state", "completed"), 10);
        metricTestHelper.assertMetric(distributedTaskMetricHelper.buildName("saga", "state", "completed_cleaning"), 14);
        metricTestHelper.assertMetric(distributedTaskMetricHelper.buildName("saga", "state", "completed_not_cleaned"), 18);
        metricTestHelper.assertMetric(distributedTaskMetricHelper.buildName("saga", "state", "expired"), 13);
        metricTestHelper.assertMetric(distributedTaskMetricHelper.buildName("saga", "state", "expired_not_cleaned"), 10);

        metricTestHelper.assertMetricToContain(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "active"),
            METER_BY_SAGA_NAME_AND_COUNT,
            Tuple.tuple(generateSagaName(ACTIVE, 0), 5),
            Tuple.tuple(generateSagaName(ACTIVE, 1), 4),
            Tuple.tuple(generateSagaName(ACTIVE, 2), 3)
        );
        metricTestHelper.assertMetricToContain(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "completed"),
            METER_BY_SAGA_NAME_AND_COUNT,
            Tuple.tuple(generateSagaName(COMPLETED, 0), 4),
            Tuple.tuple(generateSagaName(COMPLETED, 1), 3),
            Tuple.tuple(generateSagaName(COMPLETED, 2), 2)
        );
        metricTestHelper.assertMetricToContain(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "completed_cleaning"),
            METER_BY_SAGA_NAME_AND_COUNT,
            Tuple.tuple(generateSagaName(COMPLETED_CLEANING, 0), 5),
            Tuple.tuple(generateSagaName(COMPLETED_CLEANING, 1), 4),
            Tuple.tuple(generateSagaName(COMPLETED_CLEANING, 2), 3)
        );
        metricTestHelper.assertMetricToContain(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "completed_not_cleaned"),
            METER_BY_SAGA_NAME_AND_COUNT,
            Tuple.tuple(generateSagaName(COMPLETED_NOT_CLEANED, 0), 6),
            Tuple.tuple(generateSagaName(COMPLETED_NOT_CLEANED, 1), 5),
            Tuple.tuple(generateSagaName(COMPLETED_NOT_CLEANED, 2), 4)
        );
        metricTestHelper.assertMetricToContain(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "expired"),
            METER_BY_SAGA_NAME_AND_COUNT,
            Tuple.tuple(generateSagaName(EXPIRED, 0), 5),
            Tuple.tuple(generateSagaName(EXPIRED, 1), 4),
            Tuple.tuple(generateSagaName(EXPIRED, 2), 3)
        );
        metricTestHelper.assertMetricToContain(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "expired_not_cleaned"),
            METER_BY_SAGA_NAME_AND_COUNT,
            Tuple.tuple(generateSagaName(EXPIRED_NOT_CLEANED, 0), 4),
            Tuple.tuple(generateSagaName(EXPIRED_NOT_CLEANED, 1), 3),
            Tuple.tuple(generateSagaName(EXPIRED_NOT_CLEANED, 2), 2)
        );
    }

    @SneakyThrows
    @Test
    void shouldRemoveMetricsWhenRemovedFromTopN() {
        //when
        when(plannerService.isActive()).thenReturn(true);
        populateSagaEntities(ACTIVE, Map.of(0, 5, 1, 4, 2, 3, 3, 1, 4, 1));
        sagaStatService.calculateStat();
        metricTestHelper.assertMetricToContain(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "active"),
            METER_BY_SAGA_NAME_AND_COUNT,
            Tuple.tuple(generateSagaName(ACTIVE, 0), 5),
            Tuple.tuple(generateSagaName(ACTIVE, 1), 4),
            Tuple.tuple(generateSagaName(ACTIVE, 2), 3)
        );

        //do
        populateSagaEntities(ACTIVE, Map.of(3, 6, 4, 5));
        sagaStatService.calculateStat();

        //verify
        metricTestHelper.assertMetricToContain(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "active"),
            METER_BY_SAGA_NAME_AND_COUNT,
            Tuple.tuple(generateSagaName(ACTIVE, 3), 7),
            Tuple.tuple(generateSagaName(ACTIVE, 4), 6),
            Tuple.tuple(generateSagaName(ACTIVE, 0), 5)
        );
        metricTestHelper.assertMetricNotExists(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "active"),
            METER_BY_SAGA_NAME,
            Tuple.tuple(generateSagaName(ACTIVE, 1)),
            Tuple.tuple(generateSagaName(ACTIVE, 2))
        );
    }

    @SneakyThrows
    @Test
    void shouldRemoveMetricsWhenInactive() {
        //when
        when(plannerService.isActive()).thenReturn(true);
        populateSagaEntities(ACTIVE, Map.of(0, 1));
        populateSagaEntities(COMPLETED, Map.of(0, 1));
        populateSagaEntities(COMPLETED_CLEANING, Map.of(0, 1));
        populateSagaEntities(COMPLETED_NOT_CLEANED, Map.of(0, 1));
        populateSagaEntities(EXPIRED, Map.of(0, 1));
        populateSagaEntities(EXPIRED_NOT_CLEANED, Map.of(0, 1));
        sagaStatService.calculateStat();

        //do
        when(plannerService.isActive()).thenReturn(false);
        sagaStatService.calculateStat();

        //verify
        metricTestHelper.assertMetricNotExists(distributedTaskMetricHelper.buildName("saga", "state", "all"));
        metricTestHelper.assertMetricNotExists(distributedTaskMetricHelper.buildName("saga", "state", "active"));
        metricTestHelper.assertMetricNotExists(distributedTaskMetricHelper.buildName("saga", "state", "completed"));
        metricTestHelper.assertMetricNotExists(distributedTaskMetricHelper.buildName("saga", "state", "completed_cleaning"));
        metricTestHelper.assertMetricNotExists(distributedTaskMetricHelper.buildName("saga", "state", "completed_not_cleaned"));
        metricTestHelper.assertMetricNotExists(distributedTaskMetricHelper.buildName("saga", "state", "expired"));
        metricTestHelper.assertMetricNotExists(distributedTaskMetricHelper.buildName("saga", "state", "expired_not_cleaned"));

        metricTestHelper.assertMetricNotExists(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "active"),
            METER_BY_SAGA_NAME,
            Tuple.tuple(generateSagaName(ACTIVE, 0))
        );
        metricTestHelper.assertMetricNotExists(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "completed"),
            METER_BY_SAGA_NAME,
            Tuple.tuple(generateSagaName(ACTIVE, 0))
        );
        metricTestHelper.assertMetricNotExists(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "completed_cleaning"),
            METER_BY_SAGA_NAME,
            Tuple.tuple(generateSagaName(ACTIVE, 0))
        );
        metricTestHelper.assertMetricNotExists(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "completed_not_cleaned"),
            METER_BY_SAGA_NAME,
            Tuple.tuple(generateSagaName(ACTIVE, 0))
        );
        metricTestHelper.assertMetricNotExists(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "expired"),
            METER_BY_SAGA_NAME,
            Tuple.tuple(generateSagaName(ACTIVE, 0))
        );
        metricTestHelper.assertMetricNotExists(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "expired_not_cleaned"),
            METER_BY_SAGA_NAME,
            Tuple.tuple(generateSagaName(ACTIVE, 0))
        );
    }

    @SneakyThrows
    @Test
    void shouldReregisterMetricsWhenActiveAfterInactive() {
        //when
        when(plannerService.isActive()).thenReturn(true);
        populateSagaEntities(ACTIVE, Map.of(0, 5, 1, 4, 2, 3, 3, 1)); //5+4+3+1=13
        sagaStatService.calculateStat();
        when(plannerService.isActive()).thenReturn(false);
        sagaStatService.calculateStat();
        when(plannerService.isActive()).thenReturn(true);

        //do
        sagaStatService.calculateStat();


        //verify
        metricTestHelper.assertMetric(distributedTaskMetricHelper.buildName("saga", "state", "all"), 13);
        metricTestHelper.assertMetric(distributedTaskMetricHelper.buildName("saga", "state", "active"), 13);

        metricTestHelper.assertMetricToContain(
            distributedTaskMetricHelper.buildName("saga", "state", "topn", "active"),
            METER_BY_SAGA_NAME_AND_COUNT,
            Tuple.tuple(generateSagaName(ACTIVE, 0), 5),
            Tuple.tuple(generateSagaName(ACTIVE, 1), 4),
            Tuple.tuple(generateSagaName(ACTIVE, 2), 3)
        );
    }

    private List<SagaEntity> populateSagaEntities(AggregatedSagaStat.State state, Map<Integer, Integer> distribution) {
        return distribution.entrySet().stream()
            .flatMap(entry -> IntStream.range(0, entry.getValue())
                .mapToObj(i -> CreateSagaRequest.builder()
                    .sagaId(UUID.randomUUID())
                    .name(generateSagaName(state, entry.getKey()))
                    .rootTaskId(TaskId.builder().build())
                    .sagaPipeline(new SagaPipeline())
                    .build()
                )
                .map(sagaRequest -> sagaMapper.toEntity(sagaRequest))
                .map(sagaEntity -> sagaEntity.toBuilder()
                    .availableAfterCompletionTimeoutSec(100)
                    .build()
                )
                .map(sagaEntity -> sagaEntityPatcherByState.get(state).apply(sagaEntity))
                .map(sagaEntity -> sagaRepository.saveOrUpdate(sagaEntity))
            )
            .toList();
    }

    private String generateSagaName(AggregatedSagaStat.State state, int idx) {
        return String.join("-", "saga", state.name(), Integer.toString(idx)).toLowerCase();
    }

    private void initSagaEntityPatcherByState() {
        var now = LocalDateTime.now(clock);
        sagaEntityPatcherByState = Map.of(
            ACTIVE, sagaEntity -> sagaEntity.toBuilder()
                .createdDateUtc(now.minusSeconds(100))
                .completedDateUtc(null)
                .expirationDateUtc(now.plusSeconds(100))
                .build(),
            COMPLETED, sagaEntity -> sagaEntity.toBuilder()
                .createdDateUtc(now.minusSeconds(100))
                .completedDateUtc(now.minusSeconds(10))
                .expirationDateUtc(now.plusSeconds(100))
                .build(),
            COMPLETED_CLEANING, sagaEntity -> sagaEntity.toBuilder()
                .createdDateUtc(now.minusSeconds(100))
                .completedDateUtc(now.minusSeconds(11))
                .expirationDateUtc(now.minusSeconds(1)) //delta = calcFixedDelay*2
                .build(),
            COMPLETED_NOT_CLEANED, sagaEntity -> sagaEntity.toBuilder()
                .createdDateUtc(now.minusSeconds(100))
                .completedDateUtc(now.minusSeconds(20))
                .expirationDateUtc(now.minusSeconds(10)) //delta = calcFixedDelay*2
                .build(),
            EXPIRED, sagaEntity -> sagaEntity.toBuilder()
                .createdDateUtc(now.minusSeconds(100))
                .completedDateUtc(null)
                .expirationDateUtc(now.minusSeconds(1))  //delta = calcFixedDelay*2
                .build(),
            EXPIRED_NOT_CLEANED, sagaEntity -> sagaEntity.toBuilder()
                .createdDateUtc(now.minusSeconds(100))
                .completedDateUtc(null)
                .expirationDateUtc(now.minusSeconds(10))  //delta = calcFixedDelay*2
                .build()
        );
    }

    @SuppressWarnings("unchecked")
    protected static final Function<Meter, ?>[] METER_BY_SAGA_NAME_AND_COUNT = new Function[]{
        meter -> ((Meter) meter).getId().getTag("saga_name"),
        meter -> (int) ((Meter) meter).measure().iterator().next().getValue()
    };

    @SuppressWarnings("unchecked")
    protected static final Function<Meter, ?>[] METER_BY_SAGA_NAME = new Function[]{
        meter -> ((Meter) meter).getId().getTag("saga_name")
    };
}
package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.annotations.SagaRevertMethod;
import com.distributed_task_framework.test_service.models.RemoteOneDto;
import com.distributed_task_framework.test_service.models.RemoteTwoDto;
import com.distributed_task_framework.test_service.models.SagaRevertableDto;
import com.distributed_task_framework.test_service.models.SagaTrackId;
import com.distributed_task_framework.test_service.models.TestDataDto;
import com.distributed_task_framework.test_service.persistence.entities.Audit;
import com.distributed_task_framework.test_service.persistence.entities.TestDataEntity;
import com.distributed_task_framework.test_service.persistence.repository.AuditRepository;
import com.distributed_task_framework.test_service.persistence.repository.TestDataRepository;
import com.distributed_task_framework.test_service.services.RemoteServiceOne;
import com.distributed_task_framework.test_service.services.RemoteServiceTwo;
import com.distributed_task_framework.test_service.services.SagaFlow;
import com.distributed_task_framework.test_service.services.SagaProcessor;
import com.distributed_task_framework.test_service.services.TestSagaService;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TestSagaServiceImpl implements TestSagaService {
    private static final String TEST_DATA_MANAGEMENT = "TEST_DATA_MANAGEMENT";

    AuditRepository auditRepository;
    TestDataRepository testDataRepository;
    SagaProcessor sagaProcessor;
    RemoteServiceOne remoteServiceOne;
    RemoteServiceTwo remoteServiceTwo;

    @Lazy
    @NonFinal
    @Autowired
    TestSagaServiceImpl testSagaService;

    //naive and straightforward approach
    @Transactional
    @Override
    public Audit naiveSagaCall(TestDataDto testDataDto) {
        TestDataEntity data = TestDataEntity.builder()
                .id(testDataDto.getId())
                .version(testDataDto.getVersion())
                .data(testDataDto.getRemoteOneData())
                .build();
        testDataRepository.save(data); //will be roll back if rest calls failed

        var remoteOneDto = RemoteOneDto.builder()
                .remoteOneId(testDataDto.getRemoteServiceOneId())
                .remoteOneData(testDataDto.getRemoteOneData())
                .build();
        remoteServiceOne.create(remoteOneDto); //how to handle case when changes applied but return http call failed?

        var remoteTwoDto = RemoteTwoDto.builder()
                .remoteOneId(remoteOneDto.getRemoteOneId())
                .remoteTwoId(testDataDto.getRemoteServiceTwoId())
                .remoteTwoData(testDataDto.getRemoteTwoData())
                .build();
        remoteServiceTwo.create(remoteTwoDto); //how to handle real fail? how to rollback prev rest call?

        return auditRepository.save(Audit.builder()
                .who("I")
                .when(Instant.now())
                .what(remoteTwoDto.getRemoteTwoData())
                .build()
        );
    }

    @Override
    public void sagaCallAsyncWithoutTrackId(TestDataDto testDataDto) {
        sagaCallBase(testDataDto);
    }

    @SneakyThrows
    @Override
    public Audit sagaCall(TestDataDto testDataDto) {
        return sagaCallBase(testDataDto)
                .get(Duration.ofMinutes(1))
                .orElseThrow();
    }

    @Override
    public SagaTrackId sagaCallAsync(TestDataDto testDataDto) {
        return sagaCallBase(testDataDto)
                .trackId();
    }

    @SneakyThrows
    @Override
    public Optional<Audit> sagaCallPollResult(SagaTrackId trackId) {
        Optional<SagaFlow<Audit>> sagaProcessorFlow = sagaProcessor.getFlow(trackId, Audit.class)
                .filter(SagaFlow::isCompleted);
        if (sagaProcessorFlow.isPresent()) {
            SagaFlow<Audit> auditSagaFlow = sagaProcessorFlow.get();
            return auditSagaFlow.get();
        }
        return Optional.empty();
    }

    private SagaFlow<Audit> sagaCallBase(TestDataDto testDataDto) {
        return sagaProcessor
                .registerToRun(
                        s -> testSagaService.createLocal(s),
                        (in, out, thr) -> testSagaService.deleteLocal(in, out, thr),
                        testDataDto
                )
                .thenRun(
                        (r, s) -> testSagaService.createOnRemoteServiceOne(r, s),
                        (p, in, out, thr) -> testSagaService.deleteOnRemoteServiceOne(p, in, out, thr),
                        testDataDto
                )
                .thenRun(
                        (r, s) -> testSagaService.createOnRemoteServiceTwo(r, s),
                        (p, in, out, thr) -> testSagaService.deleteOnRemoteServiceTwo(p, in, out, thr),
                        testDataDto
                )
                .thenRun(r -> testSagaService.saveAudit(r))
                .startWithAffinity(TEST_DATA_MANAGEMENT, "" + testDataDto.getId());
    }

    @Transactional
    @SagaMethod(name = "createLocal", noRetryFor = OptimisticLockingFailureException.class)
    public SagaRevertableDto<TestDataEntity> createLocal(TestDataDto testDataDto) {
        TestDataEntity testDataEntity = TestDataEntity.builder()
                .id(testDataDto.getId())
                .version(testDataDto.getVersion())
                .data(testDataDto.getRemoteOneData() + testDataDto.getRemoteTwoData())
                .build();

        return SagaRevertableDto.<TestDataEntity>builder()
                .prevValue(testDataRepository.findById(testDataEntity.getId()).orElse(null))
                .newValue(testDataRepository.save(testDataEntity))
                .build();
    }

    @Transactional
    @SagaRevertMethod(name = "deleteLocal")
    public void deleteLocal(TestDataDto input,
                            @Nullable SagaRevertableDto<TestDataEntity> output,
                            @Nullable Throwable throwable) {
        if (throwable instanceof OptimisticLockingFailureException) {
            log.warn("deleteLocal(): data has been changed");
            return;
        }
        if (output != null && output.getPrevValue() != null) {
            TestDataEntity prevTestData = output.getPrevValue();
            prevTestData = prevTestData
                    .toBuilder()
                    .version(prevTestData.getVersion() + 1) // we upped version in createLocal
                    .build();
            testDataRepository.save(prevTestData);
        }
    }

    //demonstrate how to introduce backward incompatible changes
    @Deprecated(forRemoval = true, since = "v2 is in production")
    @SagaMethod(name = "createOnRemoteServiceOne")
    public RemoteOneDto createOnRemoteServiceOneOld(SagaRevertableDto<TestDataEntity> revertableTestDataEntity,
                                                    TestDataDto testDataDto) {
        var remoteOneDto = RemoteOneDto.builder()
                .remoteOneId(testDataDto.getRemoteServiceOneId())
                .remoteOneData(testDataDto.getRemoteOneData())
                .build();
        return remoteServiceOne.create(remoteOneDto);
    }

    @SagaMethod(name = "createOnRemoteServiceOn", version = 2)
    public RemoteOneDto createOnRemoteServiceOne(SagaRevertableDto<TestDataEntity> revertableTestDataEntity,
                                                 TestDataDto testDataDto) {
        var remoteOneDto = RemoteOneDto.builder()
                .remoteOneId(testDataDto.getRemoteServiceOneId())
                .remoteOneData(testDataDto.getRemoteOneData())
                .build();
        return remoteServiceOne.create(remoteOneDto);
    }

    @SagaRevertMethod(name = "deleteOnRemoteServiceOne")
    public void deleteOnRemoteServiceOne(@Nullable SagaRevertableDto<TestDataEntity> parentInput,
                                         TestDataDto input,
                                         @Nullable RemoteOneDto output,
                                         @Nullable Throwable throwable) {
        if (output != null) {
            remoteServiceOne.delete(output.getRemoteOneId());
        }
    }

    @SagaMethod(name = "createOnRemoteServiceTwo")
    public RemoteTwoDto createOnRemoteServiceTwo(RemoteOneDto remoteOneDto, TestDataDto testDataDto) {
        var remoteTwoDto = RemoteTwoDto.builder()
                .remoteOneId(remoteOneDto.getRemoteOneId())
                .remoteTwoId(testDataDto.getRemoteServiceTwoId())
                .remoteTwoData(testDataDto.getRemoteTwoData())
                .build();
        return remoteServiceTwo.create(remoteTwoDto);
    }

    @SagaRevertMethod(name = "deleteOnRemoteServiceTwo")
    public void deleteOnRemoteServiceTwo(@Nullable RemoteOneDto parentInput,
                                         TestDataDto input,
                                         @Nullable RemoteTwoDto output,
                                         @Nullable Throwable throwable) {
        if (output != null) {
            remoteServiceTwo.delete(output.getRemoteTwoId());
        }
    }

    @SagaMethod(name = "saveAudit")
    public Audit saveAudit(RemoteTwoDto remoteTwoDto) {
        return auditRepository.save(Audit.builder()
                .who("I")
                .when(Instant.now())
                .what(remoteTwoDto.toString())
                .build()
        );
    }
}
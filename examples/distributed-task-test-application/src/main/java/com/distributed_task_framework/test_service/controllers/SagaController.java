package com.distributed_task_framework.test_service.controllers;

import com.distributed_task_framework.test_service.persistence.entities.Audit;
import com.distributed_task_framework.test_service.models.SagaTrackId;
import com.distributed_task_framework.test_service.models.TestDataDto;
import com.distributed_task_framework.test_service.services.TestSagaService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
@RequestMapping("api/saga")
public class SagaController {
    @Autowired
    private TestSagaService testSagaService;

    @Operation(summary = "Run async dtf saga without trackId")
    @PostMapping("/async/without-track-id")
    public void sagaCallAsyncWithoutTrackId(@RequestBody(required = false) TestDataDto testDataDto) throws Exception {
        testSagaService.sagaCallAsyncWithoutTrackId(testDataDto);
    }

    @Operation(summary = "Run dtf saga sync")
    @PostMapping("/sync")
    public void runSagaSync(@RequestBody(required = false) TestDataDto testDataDto) throws Exception {
        testSagaService.runSagaSync(testDataDto);
    }

    @Operation(summary = "Run dtf saga sync and return result")
    @PostMapping("/sync/with-result")
    public Audit runSaga(@RequestBody(required = false) TestDataDto testDataDto) throws Exception {
        return testSagaService.sagaCall(testDataDto);
    }

    @Operation(summary = "Run async dtf saga")
    @PostMapping("/async")
    public SagaTrackId runSagaAsync(@RequestBody(required = false) TestDataDto testDataDto) throws Exception {
        return testSagaService.sagaCallAsync(testDataDto);
    }

    @Operation(summary = "Poll async dtf saga")
    @GetMapping("/{trackId}")
    public Optional<Audit> pollSagaAsync(@PathVariable("trackId") String trackId) {
        return testSagaService.sagaCallPollResult(SagaTrackId.of(trackId));
    }
}

package com.distributed_task_framework.perf_test.persistence.repository;

import com.distributed_task_framework.perf_test.persistence.entity.PerfTestRun;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface StressTestRunRepository extends CrudRepository<PerfTestRun, Long> {

    Optional<PerfTestRun> findByName(String name);

    @Query("""
            DELETE FROM dtf_perf_test_run
            WHERE name = :name
            """)
    @Modifying
    void deleteByName(@Param("name") String name);
}

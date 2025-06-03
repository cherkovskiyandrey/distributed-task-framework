package com.distributed_task_framework.autoconfigure.listener;

import com.distributed_task_framework.service.BackgroundJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.lang.NonNull;

@Slf4j
public class StartBackgroundJobsListener implements ApplicationListener<ApplicationStartedEvent> {

    @Override
    public void onApplicationEvent(@NonNull ApplicationStartedEvent event) {
        var backgroundJobs = event.getApplicationContext()
            .getBeansOfType(BackgroundJob.class);
        backgroundJobs.forEach((jobName, job) -> {
            log.info("Starting {} job", jobName);
            job.start();
        });
    }
}

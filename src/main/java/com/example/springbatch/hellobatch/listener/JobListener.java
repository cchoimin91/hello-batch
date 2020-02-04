package com.example.springbatch.hellobatch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

@Slf4j
public class JobListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("========== beforeJob ==========");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("========== afterJob ==========");

        if(jobExecution.getStatus() == BatchStatus.COMPLETED){
            log.info("========== JOB 실행결과 : 성공 ==========");
        }else{
            log.info("========== JOB 실행결과 : 확인필요 ==========");
        }

    }
}

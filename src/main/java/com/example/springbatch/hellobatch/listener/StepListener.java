package com.example.springbatch.hellobatch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

@Slf4j
public class StepListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("====== beforeStep ===== ");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("====== afterStep ===== ");

        if(stepExecution.getStatus() == BatchStatus.COMPLETED){
            log.info("====== STEP : {}  , 결과 : 완료!!!!", stepExecution.getStepName());
        }else{
            log.info("====== STEP : {}  , 결과 : 확인필요({}) :", stepExecution.getStepName(), stepExecution.getStatus() );
        }

        return stepExecution.getExitStatus();
    }
}

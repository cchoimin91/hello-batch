package com.example.springbatch.hellobatch.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j // log 사용을 위한 lombok 어노테이션
@RequiredArgsConstructor // 생성자 DI를 위한 lombok 어노테이션
@Configuration //모든job은 이거 등록해서 사용
public class SimpleJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory; // 생성자 DI 받음
    private final StepBuilderFactory stepBuilderFactory; // 생성자 DI 받음

    @Bean
    public Job simpleJob() {
        return jobBuilderFactory.get("simpleJob") // simpleJob 이름으로 job생성
                .start(simpleStep1(null)) // simpleStep1을 포함
                .build();
    }

    @Bean
    @JobScope
    public Step simpleStep1(@Value("#{jobParameters[requestDate]}") String requestDate) {
        return stepBuilderFactory.get("simpleStep1") // simpleStep1 이름으로 step생성
                .tasklet((contribution, chunkContext) -> { // Step 안에서 수행될 기능들을 명시
                    log.info(">>>>> 스텝1 시작");
                    log.info(">>>>> 파라미터 : {}", requestDate);
                    throw new IllegalArgumentException();
                    //return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    @JobScope
    public Step simpleStep2(@Value("#{jobParameters[requestDate]}") String requestDate) {
        return stepBuilderFactory.get("simpleStep2")
                .tasklet((contribution, chunkContext) -> {
                    log.info(">>>>> 스탭2 시작");
                    log.info(">>>>> 파라미터 = {}", requestDate);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }
}
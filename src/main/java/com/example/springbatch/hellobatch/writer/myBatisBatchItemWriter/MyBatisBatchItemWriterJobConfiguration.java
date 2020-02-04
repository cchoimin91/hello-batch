package com.example.springbatch.hellobatch.writer.myBatisBatchItemWriter;

import com.example.springbatch.hellobatch.listener.JobListener;
import com.example.springbatch.hellobatch.model.Pay;
import com.example.springbatch.hellobatch.model.Pay2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.mybatis.spring.batch.builder.MyBatisPagingItemReaderBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class MyBatisBatchItemWriterJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    private SqlSessionFactory sqlSessionFactory;

    private static final int chunkSize = 2;


    @Bean
    public Job myBatisBatchItemWriterJob(){
        log.info(">>>>>> MyBatisBatchItemWriterJob START !!!");
        return jobBuilderFactory.get("myBatisBatchItemWriterJob")
                .listener(new JobListener())
                .start(myBatisBatchItemWriterStep())
                .build();
    }

    @Bean
    public Step myBatisBatchItemWriterStep(){
        log.info(">>>>>> MyBatisBatchItemWriterStep START !!! ");
        return stepBuilderFactory.get("MyBatisBatchItemWriterStep")
                .<Pay, Pay2>chunk(chunkSize) // Pay는 Reader에서 반환할 타입, Pay2는 Writer에 파라미터로 넘어올 타입
                .reader(myBatisBatchItemWriterReader(null))
                .processor(myBatisBatchItemWriterProcessor())
                .writer(myBatisBatchItemWriter())
                .build();
    }

    @JobScope
    @Bean
    public MyBatisPagingItemReader<Pay> myBatisBatchItemWriterReader(@Value("#{jobParameters[requestDate]}") String requestDate) {
        Map<String, Object> param = new HashMap<String, Object>();
        param.put("date", requestDate);

        return new MyBatisPagingItemReaderBuilder<Pay>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId("getAllPay")
                .parameterValues(param)
                .pageSize(chunkSize)
                .build();
    }

    public ItemProcessor<Pay, Pay2> myBatisBatchItemWriterProcessor(){
        return item -> new Pay2();
    }

    @Bean
    public MyBatisBatchItemWriter<Pay2> myBatisBatchItemWriter() {
        log.info(">>>>> myBatisBatchItemWriter START !!!" );

        return new MyBatisBatchItemWriterBuilder<Pay2>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("insertPay2")
                .build();
    }

}
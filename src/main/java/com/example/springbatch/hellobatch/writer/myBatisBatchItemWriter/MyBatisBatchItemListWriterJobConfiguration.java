package com.example.springbatch.hellobatch.writer.myBatisBatchItemWriter;

import com.example.springbatch.hellobatch.listener.JobListener;
import com.example.springbatch.hellobatch.model.Pay;
import com.example.springbatch.hellobatch.model.Pay2;
import com.example.springbatch.hellobatch.proceesor.ItemListProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
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

import java.util.*;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class MyBatisBatchItemListWriterJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    private SqlSessionFactory sqlSessionFactory;

    private static final int chunkSize = 2;


    @Bean
    public Job myBatisBatchItemListWriterJob(){
        log.info(">>>>>> myBatisBatchItemListWriterJob START !!!");
        return jobBuilderFactory.get("myBatisBatchItemListWriterJob")
                .listener(new JobListener())
                .start(myBatisBatchItemListWriterStep())
                .build();
    }

    @Bean
    public Step myBatisBatchItemListWriterStep(){
        log.info(">>>>>> myBatisBatchItemListWriterStep START !!! ");
        return stepBuilderFactory.get("myBatisBatchItemListWriterStep")
                .<Pay, List<Pay2>>chunk(chunkSize) // Pay는 Reader에서 반환할 타입, Pay2는 Writer에 파라미터로 넘어올 타입
                .reader(myBatisBatchItemListWriterReader(null))
                .processor(processor2())
                .writer(myBatisBatchItemListWriter())
                .build();
    }

    @JobScope
    @Bean
    public MyBatisPagingItemReader<Pay> myBatisBatchItemListWriterReader(@Value("#{jobParameters[requestDate]}") String requestDate) {
        Map<String, Object> param = new HashMap<String, Object>();
        param.put("date", requestDate);

        return new MyBatisPagingItemReaderBuilder<Pay>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId("getAllPay")
                .parameterValues(param)
                .pageSize(chunkSize)
                .build();
    }

    public ItemProcessor<Pay, List<Pay2>> processor2(){
           return new ItemListProcessor();
    }

    /**
     *  List<T> 전달
     */
    public MybatisBatchListWriter<Pay2> myBatisBatchItemListWriter() {
        log.info(">>>>> myBatisBatchItemListWriter START !!!" );

        MyBatisBatchItemWriter<Pay2> myBatisBatchItemWriter = new MyBatisBatchItemWriter<>();
        myBatisBatchItemWriter.setSqlSessionFactory(sqlSessionFactory);
        myBatisBatchItemWriter.setStatementId("insertPay2");

        return new MybatisBatchListWriter<>(myBatisBatchItemWriter);
    }

}
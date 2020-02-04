package com.example.springbatch.hellobatch.reader.myBatisPaging;

import com.example.springbatch.hellobatch.model.Pay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
import org.mybatis.spring.batch.builder.MyBatisPagingItemReaderBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * http://mybatis.org/spring/apidocs/org/mybatis/spring/batch/builder/MyBatisPagingItemReaderBuilder.html
 */
@Slf4j
@RequiredArgsConstructor
@Configuration
public class MybatisPagingItemReaderJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    private SqlSessionFactory sqlSessionFactory;

    private static final int chunkSize = 2;



    @Bean
    public Job myBatisPagingItemReaderJob(){
        log.info(">>>>>> myBatisPagingItemReaderJob START !!!");
        return jobBuilderFactory.get("myBatisPagingItemReaderJob")
                .start(myBatisPagingItemReaderStep())
                .build();
    }

    @Bean
    public Step myBatisPagingItemReaderStep(){
        log.info(">>>>>> myBatisPagingItemReaderStep START !!! ");
        return stepBuilderFactory.get("myBatisPagingItemReaderStep")
                .<Pay, Pay>chunk(chunkSize)
                .reader(myBatisPagingItemReader(null))
                .writer(myBatisPagingItemWriter())
                .build();
    }

    @JobScope
    @Bean
    public MyBatisPagingItemReader<Pay> myBatisPagingItemReader(@Value("#{jobParameters[requestDate]}") String requestDate) {
        log.info(">>>>>> myBatisPagingItemReader START !!!");
        log.info(">>>>>> requestDate : {}", requestDate);

        Map<String, Object> param = new HashMap<String, Object>();
        param.put("date", requestDate);

        return new MyBatisPagingItemReaderBuilder<Pay>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId("getAllPay")
                .parameterValues(param)
                .pageSize(chunkSize)
                .build();
    }

    private ItemWriter<Pay> myBatisPagingItemWriter() {
        log.info(">>>>> myBatisPagingItemWriter START !!!" );
        return list -> {
            for (Pay pay: list) {
                log.info(">>>>>> Current Pay={}", pay);
            }
        };
    }
}

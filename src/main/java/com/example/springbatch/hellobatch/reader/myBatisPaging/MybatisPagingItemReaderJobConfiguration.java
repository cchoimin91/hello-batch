package com.example.springbatch.hellobatch.reader.myBatisPaging;

import com.example.springbatch.hellobatch.mapper.PayMapper;
import com.example.springbatch.hellobatch.model.Pay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
import org.mybatis.spring.batch.builder.MyBatisPagingItemReaderBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
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

    private SqlSessionFactory sqlSessionFactory;
    private SqlSessionTemplate sqlSessionTemplate;

    @Autowired
    private PayMapper payMapper;

    private static final  int chunkSize = 2;

    @Bean
    public Job myBatisPagingItemReaderJob(){
        return jobBuilderFactory.get("myBatisPaingItemReaderJob")
                .start(myBatisPagingItemReaderStep())
                .build();
    }

    @Bean
    public Step myBatisPagingItemReaderStep(){
        return stepBuilderFactory.get("myBatisPagingItemReaderStep")
                .<Pay, Pay>chunk(chunkSize)
                .reader(myBatisPagingItemReader())
                .writer(myBatisPagingItemWriter())
                .build();
    }

    @Bean
    public MyBatisPagingItemReader<Pay> myBatisPagingItemReader() {
        log.info(">>>>> myBatisPagingItemReader START !!!");
        Map<String, Object> param = new HashMap<String, Object>();
        param.put("date", "2020-01-01");

        return new MyBatisPagingItemReaderBuilder<Pay>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId("com.example.springbatch.hellobatch.mapper.PayMapper.getAllPay")
                .parameterValues(param)
                .pageSize(chunkSize)
                .build();
    }

    private ItemWriter<Pay> myBatisPagingItemWriter() {
        return list -> {
            for (Pay pay: list) {
                log.info("Current Pay={}", pay);
            }
        };
    }
}

package com.example.springbatch.hellobatch.writer.jdbcBatchWriter;

import com.example.springbatch.hellobatch.model.Pay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;

/**
 * JdbcBatchItemWriter
 *
 * 작동방식
 * 1) 쿼리모음 (chunkSize만큼 모음)
 * 2) 모아놓은 쿼리 한번에 전송
 * 3) 받은 쿼리 실행
 *
 * 특징
 * 영속성을 사용하는 경우(JPA, Hibernate) flush(), session.clear()를 해줘야함
 *
 */
@Slf4j
@RequiredArgsConstructor
@Configuration
public class JdbcBatchItemWriterJobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource; // DataSource DI

    private static final int chunkSize = 2;

    @Bean
    public Job jdbcBatchItemWriterJob() {
        return jobBuilderFactory.get("jdbcBatchItemWriterJob")
                .start(jdbcBatchItemWriterStep())
                .build();
    }

    @Bean
    public Step jdbcBatchItemWriterStep() {
        log.info(">>>>>> jdbcBatchItemWriterStep START!!!");
        return stepBuilderFactory.get("jdbcBatchItemWriterStep")
                .<Pay, Pay>chunk(chunkSize)
                .reader(jdbcBatchItemWriterReader())
                .writer(jdbcBatchItemWriter())
                .build();
    }

    @Bean
    public JdbcCursorItemReader<Pay> jdbcBatchItemWriterReader() {
        log.info(">>>>> jdbcBatchItemWriterReader START !!!! ");
        JdbcCursorItemReader<Pay> jdbcCursorItemReader =   new JdbcCursorItemReaderBuilder<Pay>()
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Pay.class))
                .sql("SELECT id, amount, tx_name, tx_date_time FROM pay")
                .name("jdbcBatchItemWriter")
                .build();
                //.afterPropertiesSet(); writer들이 실행되기 위해 필요한 필수값들이 제대로 세팅되어있는지 체크해줌
        return jdbcCursorItemReader;
    }

    /**
     * reader에서 넘어온 데이터를 하나씩 출력하는 writer
     *
     * JdbcBatchItemWriterBuilder
     * .assertUpdate() // 최소1개의 항목이 update or delete되지 않을 경우 throw할지 여부를 설정(기본 true) EmptyResultDataAccessException 발생
     * .columnMapped() // Map<key,value> 기반으로 insert SQL의 value를 맵핑함
     * .beanMapped() // Pojo기반으로 insert SQL의 value를 맵핑함
     *
     */
    @Bean // beanMapped()을 사용할때는 필수
    public JdbcBatchItemWriter<Pay> jdbcBatchItemWriter() {
        log.info(">>>>> jdbcBatchItemWriter START !!! ");
        JdbcBatchItemWriter<Pay> jdbcBatchItemWriter = new JdbcBatchItemWriterBuilder<Pay>()
                .dataSource(dataSource)
                .sql("insert into pay2(amount, tx_name, tx_date_time) values (:amount, :txName, :txDateTime)")
                .beanMapped() // dto의 getter에 의해 맵핑됨
                .build();
        return jdbcBatchItemWriter;
    }
}
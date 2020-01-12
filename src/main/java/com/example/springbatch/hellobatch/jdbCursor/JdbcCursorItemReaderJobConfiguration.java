package com.example.springbatch.hellobatch.jdbCursor;

import com.example.springbatch.hellobatch.model.Pay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;

/**
 * JDBC Cursor 특징
 * - Streaming으로 데이터 처리
 * - 데이터를 striming
 * - reader, processor , writer가 Chunk단위로 수행되고 주기적으로 commit됨 이는 고성능 배치처리의 핵심
 * - JdbcPagingItemReader에 비해 더 많은 메모리를 사용하고 더 빠름
 * - 대량데이터가 아니고, 멀티스레드 환경이 아닌 곳에서는 매우 적합
 *
 * CursorItemReader 주의사항
 * - db와 socketTimeOut을 충분히 큰 값으로 설정해야 함
 * - 왜? cursor는 하나의 connection을 사용하기 때문
 * - batch수행이 오래 걸리는 경우 PagingItemReader를 사용하는게 나음 (Cursor는 한번에 가져오는 데이터 양이 너무 많으면 batch 뻗음)
 * - 왜? Paging의 경우 한페이지를 읽을때 마다 connection을 맺고 끊음. 아무리 많은 데이터라도 timeout과 부하없이 수행 가능
 *
 *
 */
@Slf4j
@RequiredArgsConstructor
@Configuration
public class JdbcCursorItemReaderJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource; // DataSource DI

    private static final int chunkSize = 2;

    @Bean
    public Job jdbcCursorItemReaderJob() {
        return jobBuilderFactory.get("jdbcCursorItemReaderJob")
                .start(jdbcCursorItemReaderStep())
                .build();
    }

    @Bean
    public Step jdbcCursorItemReaderStep() {
        return stepBuilderFactory.get("jdbcCursorItemReaderStep")
                .<Pay, Pay>chunk(chunkSize) //1번째 param : Reader에서 반환할 타입, 2번째 param: Writer에 사용 할 파라미터
                .reader(jdbcCursorItemReader())
                .writer(jdbcCursorItemWriter())
                .build();
    }

    //READER
    @Bean
    public JdbcCursorItemReader<Pay> jdbcCursorItemReader() {
        log.info(">>>>> JdbcCursorItemReader START ");
        return new JdbcCursorItemReaderBuilder<Pay>()
                .fetchSize(chunkSize) // DB에서 한번에 가져올 데이터양
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Pay.class)) // 쿼리결과를 java인스턴스로 맵핑
                .sql("SELECT id, amount, txName, txDateTime FROM pay order by id")
                .name("jdbcCursorItemReader") // reader의 이름 지정
                .build();
    }

    //WRITER
    private ItemWriter<Pay> jdbcCursorItemWriter() {
        log.info(">>>>> jdbcCursorItemWriter START !!!");
        return list -> {
            for (Pay pay: list) {
                log.info("Current Pay={}", pay);
            }
        };
    }
}
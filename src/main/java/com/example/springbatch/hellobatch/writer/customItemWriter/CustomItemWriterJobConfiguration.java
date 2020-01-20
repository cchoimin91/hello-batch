package com.example.springbatch.hellobatch.writer.customItemWriter;

import com.example.springbatch.hellobatch.model.Pay;
import com.example.springbatch.hellobatch.model.Pay2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class CustomItemWriterJobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    private static final int chunkSize = 2;

    @Bean
    public Job customItemWriterJob() throws Exception {
        return jobBuilderFactory.get("customItemWriterJob")
                .start(customItemWriterStep())
                .build();
    }

    @Bean
    public Step customItemWriterStep() throws Exception {
        return stepBuilderFactory.get("customItemWriterStep")
                .<Pay, Pay2>chunk(chunkSize)
                .reader(customItemWriterReader())
                .processor(customItemWriterProcessor())
                .writer(customItemWriter())
                .build();
    }

    @Bean
    public JdbcPagingItemReader<Pay> customItemWriterReader() throws Exception {
        return new JdbcPagingItemReaderBuilder<Pay>()
                .name("customItemWriterReader")
                .dataSource(dataSource)
                .pageSize(chunkSize)
                .rowMapper(new BeanPropertyRowMapper<>(Pay.class))
                .queryProvider(createCustomWriterQueryProvider())
                .build();
    }

    @Bean
    public PagingQueryProvider createCustomWriterQueryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("id, amount, tx_name, tx_date_time");
        queryProvider.setFromClause("from pay");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);
        return queryProvider.getObject();
    }

    @Bean
    public ItemProcessor<Pay, Pay2> customItemWriterProcessor() {
        return pay -> new Pay2(pay.getAmount(), pay.getTxName(), pay.getTxDateTime());
    }

    @Bean
    public ItemWriter<Pay2> customItemWriter() {
        /*
        return new ItemWriter<Pay2>() {
            @Override
            public void write(List<? extends Pay2> items) throws Exception { // writer 구현체 생성
                for (Pay2 item : items) {
                    System.out.println(">>>>>"+item);
                }
            }
        };*/

        return items -> {
            for(Pay2 item : items){
                System.out.println(">>>>> customItemWriter :" + item);
            }
        };
    }
}

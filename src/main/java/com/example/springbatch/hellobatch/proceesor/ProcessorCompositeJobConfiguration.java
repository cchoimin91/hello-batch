package com.example.springbatch.hellobatch.proceesor;

import com.example.springbatch.hellobatch.model.Teacher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 주로 사용하는 용도의 Processor를 미리 클래스로 만들어서 제공해주고 있음 총3개정도
 * CompositeItemProcessor : temProcessor간의 체이닝을 지원하는 Processor
 * ItemProcessorAdapter (거의 사용안함, 대부분 직접 구현해서 사용)
 * ValidatingItemProcessor (거의 사용안함, 대부분 직접 구현해서 사용)
 *
 *  변환 or 필터가 2번이상 필요하다면?
 *  하나의 processor에서 구현하면 너무 역할이 커짐 그래서 CompositeItemProcessor을 사용
 */
@Slf4j
@RequiredArgsConstructor
@Configuration
public class ProcessorCompositeJobConfiguration {

    public static final String JOB_NAME = "processorCompositeBatch";
    public static final String BEAN_PREFIX = JOB_NAME + "_";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    private int chunkSize = 2;

    @Bean(JOB_NAME)
    public Job job() throws Exception {
        return jobBuilderFactory.get(JOB_NAME)
                .preventRestart()
                .start(step())
                .build();
    }

    @Bean(BEAN_PREFIX + "step")
    @JobScope
    public Step step() throws Exception {
        return stepBuilderFactory.get(BEAN_PREFIX + "step")
                .<Teacher, String>chunk(chunkSize)
                .reader(compositeReader())
                .processor(compositeProcessor())
                .writer(compositeWriter())
                .build();
    }

    @Bean
    public JdbcPagingItemReader <Teacher> compositeReader() throws Exception {
        return new JdbcPagingItemReaderBuilder<Teacher>()
                .name(BEAN_PREFIX+"reader")
                .dataSource(dataSource)
                .fetchSize(chunkSize)
                .rowMapper(new BeanPropertyRowMapper<>(Teacher.class))
                .queryProvider(createProcessorCompositeProvider())
                .build();
    }

    @Bean
    public PagingQueryProvider createProcessorCompositeProvider() throws Exception{
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("id , name");
        queryProvider.setFromClause("teacher");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        return queryProvider.getObject();
    }

    @Bean
    public CompositeItemProcessor compositeProcessor() {
        // delegates 포함된 모든 ItemProcessor 같은 제너릭 타입을 가져야함
        List<ItemProcessor> delegates = new ArrayList<>(2);
        delegates.add(processor1());
        delegates.add(processor2());

        CompositeItemProcessor processor = new CompositeItemProcessor<>();

        processor.setDelegates(delegates);

        return processor;
    }

    public ItemProcessor<Teacher, String> processor1() {
        return Teacher::getName;
    }

    public ItemProcessor<String, String> processor2() {
        return name -> "안녕하세요. "+ name + "입니다.";
    }

    private ItemWriter<String> compositeWriter() {
        return items -> {
            for (String item : items) {
                log.info("Teacher Name={}", item);
            }
        };
    }
}
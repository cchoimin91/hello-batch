package com.example.springbatch.hellobatch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * --job.name=processorCompositeBatch test=1
 */
@EnableBatchProcessing
@SpringBootApplication
public class HellobatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(HellobatchApplication.class, args);
    }

}

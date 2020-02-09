package com.example.springbatch.hellobatch.writer.myBatisBatchItemWriter;

import org.mybatis.spring.batch.MyBatisBatchItemWriter;

import java.util.ArrayList;
import java.util.List;

public class MybatisBatchListWriter<T> extends MyBatisBatchItemWriter<List<T>> {

    private MyBatisBatchItemWriter<T> myBatisBatchItemWriter;

    public MybatisBatchListWriter(MyBatisBatchItemWriter<T> myBatisBatchItemWriter) {
        this.myBatisBatchItemWriter = myBatisBatchItemWriter;
    }

    @Override
    public void write(List<? extends List<T>> items) {
        List<T> totalList = new ArrayList<>();

        for (List<T> list : items) {
            totalList.addAll(list);
        }

        myBatisBatchItemWriter.write(totalList);

    }

}



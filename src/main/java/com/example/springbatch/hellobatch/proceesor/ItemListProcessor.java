package com.example.springbatch.hellobatch.proceesor;

import com.example.springbatch.hellobatch.model.Pay;
import com.example.springbatch.hellobatch.model.Pay2;
import org.springframework.batch.item.ItemProcessor;

import java.util.Arrays;
import java.util.List;

public class ItemListProcessor implements ItemProcessor<Pay, List<Pay2>> {

    @Override
    public List<Pay2> process(Pay item) throws Exception {
        return Arrays.asList(
                new Pay2(100L, item.getTxName())
                ,new Pay2(200L, item.getTxName())
                );
    }
}

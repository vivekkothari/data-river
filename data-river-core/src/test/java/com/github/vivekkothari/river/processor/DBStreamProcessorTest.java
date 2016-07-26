package com.github.vivekkothari.river.processor;

import com.github.vivekkothari.river.bean.MessageKey;
import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Timestamp;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author vivek.kothari on 26/07/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class DBStreamProcessorTest {

    @Mock
    private IRiverService riverService;

    private DBStreamProcessor dbStreamProcessor;

    @Test
    public void process() throws Exception {
        dbStreamProcessor = new DBStreamProcessor(riverService);
        MessageKey key = new MessageKey();
        key.setTable("table");
        key.setDatabase("database");

        MessageValue value = new MessageValue();
        value.setDatabase(key.getDatabase());
        value.setTable(key.getTable());
        value.setCommitId(123L);
        value.setCommitted(true);
        value.setTimestamp(new Timestamp(System.currentTimeMillis()));
        value.setData(Maps.newHashMap());
        value.setOld(Maps.newHashMap());

        TransformerFactory.builder()
                          .backFillers(ImmutableMap.of("river1", Mockito.mock(IBackFiller.class)))
                          .enrichers(ImmutableMap.of("river1", Mockito.mock(IEnricher.class)))
                          .filters(ImmutableMap.of("river1", Mockito.mock(IFilter.class)))
                          .build();

        dbStreamProcessor.process(key, value, "maxwell", 0, 0L);
        verify(riverService, times(1)).process(value, "river1");

    }

}
package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author vivek.kothari on 27/07/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class RiverServiceImplTest {

    @Mock
    private IFilter filter;

    private RiverServiceImpl riverService;

    @Mock
    private IPersister persister;

    @Mock
    private IEnricher enricher;

    @Before
    public void before() {
        TransformerFactory.builder()
                          .filter("river1", filter)
                          .enricher("river1", enricher)
                          .backFiller("river1", Mockito.mock(IBackFiller.class))
                          .build();
    }

    @After
    public void after() {
        TransformerFactory.INSTANCE = null;
    }

    @Test
    public void process() throws Exception {
        riverService = new RiverServiceImpl(persister);
        when(filter.shouldProcess(any())).thenReturn(true);
        final MessageValue messageValue = new MessageValue();
        when(enricher.enrich(messageValue)).thenReturn(messageValue);
        riverService.process(messageValue, "river1");
        verify(persister, times(1)).persist(messageValue, "river1");
        verify(enricher, times(1)).enrich(messageValue);

        reset(enricher, filter, persister);

        when(filter.shouldProcess(any())).thenReturn(false);
        riverService.process(messageValue, "river1");

        verify(persister, times(0)).persist(messageValue, "river1");
        verify(enricher, times(0)).enrich(messageValue);
    }

}
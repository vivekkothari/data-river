package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class NoopEnricherTest {

    @Test
    public void enrich() throws Exception {
        NoopEnricher enricher = new NoopEnricher();
        MessageValue value = new MessageValue();
        final MessageValue enrich = enricher.enrich(value);
        Assert.assertEquals(value, enrich);
    }

}
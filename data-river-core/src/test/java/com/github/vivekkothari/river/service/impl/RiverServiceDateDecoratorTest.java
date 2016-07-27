package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IRiverService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author vivek.kothari on 27/07/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class RiverServiceDateDecoratorTest {

    @Mock
    private IRiverService delegate;

    private RiverServiceDateDecorator decorator;

    @Test
    public void process_data() throws Exception {
        decorator = new RiverServiceDateDecorator(delegate, Sets.newHashSet("created_at"));
        MessageValue value = new MessageValue();
        value.setData(new HashMap<>(ImmutableMap.of("created_at", "2016-04-01 00:00:00", "updated_at", new Date())));
        decorator.process(value, "river1");

        final Map<String, Object> data = value.getData();
        Assert.assertNotNull(data);
        Assert.assertEquals(true, data.get("created_at")
                                      .getClass() == Date.class);
        Assert.assertEquals(true, data.get("updated_at")
                                      .getClass() == Date.class);
    }

    @Test
    public void process_old() throws Exception {
        decorator = new RiverServiceDateDecorator(delegate, Sets.newHashSet("created_at", "updated_at"));
        MessageValue value = new MessageValue();
        value.setData(new HashMap<>(ImmutableMap.of("created_at", "2016-04-01 00:00:00", "updated_at", new Date())));
        value.setOld(new HashMap<>(ImmutableMap.of("updated_at", "2016-04-01 00:00:00", "created_at", new Date())));
        decorator.process(value, "river1");

        final Map<String, Object> data = value.getOld();
        Assert.assertNotNull(data);
        Assert.assertEquals(true, data.get("created_at")
                                      .getClass() == Date.class);
        Assert.assertEquals(true, data.get("updated_at")
                                      .getClass() == Date.class);
    }

}
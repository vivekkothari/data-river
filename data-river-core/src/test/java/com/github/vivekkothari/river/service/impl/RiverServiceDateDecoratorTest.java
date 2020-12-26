package com.github.vivekkothari.river.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IRiverService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Date;
import java.util.HashMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author vivek.kothari on 27/07/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class RiverServiceDateDecoratorTest {

  @Mock
  private IRiverService delegate;

  private RiverServiceDateDecorator decorator;

  @Test
  public void process_data() {
    decorator = new RiverServiceDateDecorator(delegate, Sets.newHashSet("created_at"));
    var value = new MessageValue();
    value.setData(new HashMap<>(
        ImmutableMap.of("created_at", "2016-04-01 00:00:00", "updated_at", new Date())));
    decorator.process(value, "river1");

    final var data = value.getData();
    assertNotNull(data);
    assertEquals(true, data.get("created_at")
        .getClass() == Date.class);
    assertEquals(true, data.get("updated_at")
        .getClass() == Date.class);
  }

  @Test
  public void process_data_invalid() {
    decorator = new RiverServiceDateDecorator(delegate, Sets.newHashSet("created_at"));
    var value = new MessageValue();
    value.setData(new HashMap<>(
        ImmutableMap.of("created_at", "2016-04-0100:00:00", "updated_at", new Date())));
    decorator.process(value, "river1");

    final var data = value.getData();
    assertNotNull(data);
    assertEquals(true, data.get("created_at")
        .getClass() == String.class);
    assertEquals(true, data.get("updated_at")
        .getClass() == Date.class);
  }

  @Test
  public void process_data_null() {
    decorator = new RiverServiceDateDecorator(delegate, Sets.newHashSet("created_at"));
    var value = new MessageValue();
    value.setData(new HashMap<>(ImmutableMap.of("created_at", "null", "updated_at", new Date())));
    decorator.process(value, "river1");

    final var data = value.getData();
    assertNotNull(data);
    assertNull(data.get("created_at"));
    assertEquals(true, data.get("updated_at")
        .getClass() == Date.class);
  }

  @Test
  public void process_old() {
    decorator = new RiverServiceDateDecorator(delegate,
        Sets.newHashSet("created_at", "updated_at"));
    var value = new MessageValue();
    value.setData(new HashMap<>(
        ImmutableMap.of("created_at", "2016-04-01 00:00:00", "updated_at", new Date())));
    value.setOld(new HashMap<>(
        ImmutableMap.of("updated_at", "2016-04-01 00:00:00", "created_at", new Date())));
    decorator.process(value, "river1");

    final var data = value.getOld();
    assertNotNull(data);
    assertEquals(true, data.get("created_at")
        .getClass() == Date.class);
    assertEquals(true, data.get("updated_at")
        .getClass() == Date.class);
  }

}
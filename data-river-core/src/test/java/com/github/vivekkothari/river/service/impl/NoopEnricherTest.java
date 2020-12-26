package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class NoopEnricherTest {

  @Test
  public void enrich() {
    var enricher = new NoopEnricher();
    var value = new MessageValue();
    final var enrich = enricher.enrich(value);
    Assert.assertEquals(value, enrich);
  }

}
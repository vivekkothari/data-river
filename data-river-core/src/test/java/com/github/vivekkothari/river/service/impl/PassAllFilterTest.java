package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class PassAllFilterTest {

  @Test
  public void shouldProcess() {
    var filter = new PassAllFilter();
    final var shouldProcess = filter.shouldProcess(new MessageValue());
    Assert.assertEquals(true, shouldProcess);
  }

}
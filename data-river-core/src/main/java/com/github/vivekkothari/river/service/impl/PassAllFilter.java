package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IFilter;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class PassAllFilter
    implements IFilter {

  @Override
  public boolean shouldProcess(final MessageValue messageValue) {
    return true;
  }
}

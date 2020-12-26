package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IEnricher;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class NoopEnricher
    implements IEnricher {

  @Override
  public MessageValue enrich(final MessageValue messageValue) {
    return messageValue;
  }

}

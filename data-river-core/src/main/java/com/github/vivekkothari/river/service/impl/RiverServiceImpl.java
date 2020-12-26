package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IPersister;
import com.github.vivekkothari.river.service.IRiverService;
import com.github.vivekkothari.river.service.TransformerFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * @author vivek.kothari on 16/06/16.
 */
@Slf4j
public class RiverServiceImpl
    implements IRiverService {

  private final IPersister persister;

  public RiverServiceImpl(final IPersister persister) {
    this.persister = persister;
  }

  @Override
  public void process(final MessageValue messageValue, final String riverType) {
    final var transformerFactory = TransformerFactory.INSTANCE;
    final var filter = transformerFactory.filter(riverType);
    if (filter.shouldProcess(messageValue)) {
      log.info("processing message {} for riverType {}", messageValue, riverType);
      final var enricher = transformerFactory.enricher(riverType);
      final var enrich = enricher.enrich(messageValue);
      persister.persist(enrich, riverType);
    }
  }
}

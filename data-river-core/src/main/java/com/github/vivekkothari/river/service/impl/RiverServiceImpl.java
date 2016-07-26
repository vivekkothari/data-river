package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.*;
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
        final TransformerFactory transformerFactory = TransformerFactory.INSTANCE;
        final IFilter filter = transformerFactory.filter(riverType);
        if (filter.shouldProcess(messageValue)) {
            log.info("processing message {} for riverType {}", messageValue, riverType);
            final IEnricher enricher = transformerFactory.enricher(riverType);
            final MessageValue enrich = enricher.enrich(messageValue);
            persister.persist(enrich, riverType);
        }
    }
}

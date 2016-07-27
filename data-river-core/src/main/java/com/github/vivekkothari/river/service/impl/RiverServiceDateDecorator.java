package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IEnricher;
import com.github.vivekkothari.river.service.IRiverService;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author vivek.kothari on 20/06/16.
 */
@Slf4j
public class RiverServiceDateDecorator
        implements IRiverService {

    private final IRiverService delegate;
    /**
     * Date fields which will be inspected for date conversion.
     */
    private final Set<String> dateFields;

    public RiverServiceDateDecorator(final IRiverService delegate, final Set<String> dateFields) {
        this.delegate = delegate;
        this.dateFields = ImmutableSet.copyOf(dateFields);
    }

    @Override
    public void process(final MessageValue messageValue, final String riverType) {
        final Map<String, Object> data = messageValue.getData();
        data.entrySet()
            .stream()
            .filter(entry -> dateFields.contains(entry.getKey()))
            .forEachOrdered(entry -> {
                final Object value = entry.getValue();
                if (!(value instanceof Date)) {
                    updateEntry(entry);
                }
            });
        Optional.ofNullable(messageValue.getOld())
                .ifPresent(old -> old.entrySet()
                                     .stream()
                                     .filter(entry -> dateFields.contains(entry.getKey()))
                                     .forEachOrdered(entry -> {
                                         final Object value = entry.getValue();
                                         if (!(value instanceof Date)) {
                                             updateEntry(entry);
                                         }
                                     }));
        delegate.process(messageValue, riverType);
    }

    private void updateEntry(final Map.Entry<String, Object> entry) {
        final Object value = entry.getValue();
        try {
            if (value != null && !"null".equals(value.toString())) {
                log.info("Converting date value for {}, whose original value is {}", entry.getKey(), value);
                entry.setValue(IEnricher.dtf.get()
                                            .parse(String.valueOf(value)));
            } else {
                entry.setValue(null);
            }
        } catch (ParseException e) {
            log.error("Error paring date", e);
        }
    }
}

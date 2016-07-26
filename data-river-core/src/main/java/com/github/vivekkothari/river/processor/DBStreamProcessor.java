package com.github.vivekkothari.river.processor;

import com.datasift.dropwizard.kafka.consumer.MessageProcessor;
import com.github.vivekkothari.river.bean.MessageKey;
import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IRiverService;
import com.github.vivekkothari.river.service.TransformerFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * @author vivek.kothari on 15/05/16.
 */
@Slf4j
public class DBStreamProcessor
        extends MessageProcessor<MessageKey, MessageValue> {

    private final IRiverService riverService;

    public DBStreamProcessor(final IRiverService riverService) {
        this.riverService = riverService;
    }

    @Override
    public void process(final MessageKey key, final MessageValue message, final String topic, final int partition, final long offset) {
        try {
            log.debug("Got message from topic {}, partition {}, offset is {}", topic, partition, offset);
            log.debug("message {}", message);
            for (final String riverType : TransformerFactory.INSTANCE.getRiverTypes()) {
                riverService.process(message, riverType);
            }
        } catch (Exception e) {
            log.error("Error in message " + message, e);
        }
    }
}

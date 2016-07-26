package com.github.vivekkothari.river.service;

import com.github.vivekkothari.river.bean.MessageValue;

/**
 * @author vivek.kothari on 16/06/16.
 */
@FunctionalInterface
public interface IRiverService {

    void process(final MessageValue messageValue, final String riverType);

}

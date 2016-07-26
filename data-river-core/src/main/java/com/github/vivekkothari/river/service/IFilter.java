package com.github.vivekkothari.river.service;

import com.github.vivekkothari.river.bean.MessageValue;

/**
 * @author vivek.kothari on 30/05/16.
 */
@FunctionalInterface
public interface IFilter {

    /**
     * @param messageValue
     *         incoming messageValue
     *
     * @return {@code true} if the messages needs to be processed, {@code false} if this message needs to be filtered.
     */
    boolean shouldProcess(final MessageValue messageValue);

}

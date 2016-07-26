package com.github.vivekkothari.river.service;

import com.github.vivekkothari.river.bean.MessageValue;

import java.time.LocalDateTime;
import java.util.stream.Stream;

/**
 * @author vivek.kothari on 16/06/16.
 */
public interface IBackFiller {

    /**
     * Returns a stream of elements which needs to be back-filled.
     *
     * @param startDate
     * @param endDate
     * @param parallel
     *         should the stream be parallel
     *
     * @return
     */
    Stream<MessageValue> streamElements(final LocalDateTime startDate, final LocalDateTime endDate, final boolean parallel);

    /**
     * Method to close any resources opened in the {@link #streamElements(LocalDateTime, LocalDateTime, boolean)}
     */
    void cleanup();

}

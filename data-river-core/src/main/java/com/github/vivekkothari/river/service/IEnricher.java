package com.github.vivekkothari.river.service;

import com.github.vivekkothari.river.bean.MessageValue;
import lombok.SneakyThrows;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author vivek.kothari on 30/05/16.
 */
public interface IEnricher {

    ThreadLocal<DateFormat> dtf = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"));
    ThreadLocal<DateFormat> df = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));

    /**
     * Enriches the incoming {@link MessageValue} and returns the enriched {@link MessageValue}.
     *
     * @param messageValue
     *         incoming messageValue
     *
     * @return
     */
    MessageValue enrich(final MessageValue messageValue);

    /**
     * Returns the {@link Date} on which the record was first created.
     *
     * @param messageValue
     *         incoming messageValue
     *
     * @return by default returns the "created_at" field in the incoming message.
     */
    @SneakyThrows
    default Date getRecordCreationDate(final MessageValue messageValue) {
        return (Date) messageValue.getData()
                                  .get("created_at");
    }

    /**
     * Returns the {@code id} using which it needs to be indexed in ES
     *
     * @param messageValue
     *         incoming messageValue
     *
     * @return by default returns the "id" field in the incoming message.
     */
    default String recordId(final MessageValue messageValue) {
        return String.valueOf(messageValue.getData()
                                          .get("id"));
    }

    /**
     * Returns the {@code indexName} in which this record needs to be put.
     *
     * @param messageValue
     *         incoming messageValue
     *
     * @return by default returns the index in format db_name-table_name-yyyy-MM-dd format.
     */
    default String indexName(final MessageValue messageValue) {
        return String.join("-", messageValue.getDatabase(), messageValue.getTable(), df.get()
                                                                                       .format(getRecordCreationDate(messageValue)))
                     .toLowerCase();
    }

}

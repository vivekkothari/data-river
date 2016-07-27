package com.github.vivekkothari.persister.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class DailyIndexNameGeneratorTest {

    @Test
    public void indexName() throws Exception {
        DailyIndexNameGenerator indexNameGenerator = new DailyIndexNameGenerator();
        final long l = LocalDate.of(2016, Month.APRIL, 01)
                                .atStartOfDay()
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
        final MessageValue value = new MessageValue();
        value.setDatabase("db");
        value.setTable("table");
        final String indexName = indexNameGenerator.indexName(value, new Date(l));
        Assert.assertEquals("db-table-2016-04-01", indexName);
    }

}
package com.github.vivekkothari.persister.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class DailyIndexNameGeneratorTest {

  @Test
  public void indexName() {
    var indexNameGenerator = new DailyIndexNameGenerator();
    final var l = LocalDate.of(2016, Month.APRIL, 1)
        .atStartOfDay()
        .atZone(ZoneId.systemDefault())
        .toInstant()
        .toEpochMilli();
    final var value = new MessageValue();
    value.setDatabase("db");
    value.setTable("table");
    final var indexName = indexNameGenerator.indexName(value, new Date(l));
    Assert.assertEquals("db-table-2016-04-01", indexName);
  }

}
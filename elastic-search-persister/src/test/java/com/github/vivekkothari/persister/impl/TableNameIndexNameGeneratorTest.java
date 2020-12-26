package com.github.vivekkothari.persister.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author vivek.kothari on 27/07/16.
 */
public class TableNameIndexNameGeneratorTest {

  @Test
  public void indexName() {
    final var value = new MessageValue();
    value.setDatabase("db");
    value.setTable("Table");
    var indexNameGenerator = new TableNameIndexNameGenerator();
    final var indexName = indexNameGenerator.indexName(value, new Date());
    Assert.assertEquals("table", indexName);
  }

}
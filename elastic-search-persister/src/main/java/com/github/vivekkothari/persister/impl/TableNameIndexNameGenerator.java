package com.github.vivekkothari.persister.impl;

import com.github.vivekkothari.persister.IndexNameGenerator;
import com.github.vivekkothari.river.bean.MessageValue;
import java.util.Date;

/**
 * @author vivek.kothari on 26/07/16.
 */
public class TableNameIndexNameGenerator
    implements IndexNameGenerator {

  @Override
  public String indexName(final MessageValue messageValue, final Date date) {
    return messageValue.getTable()
        .toLowerCase();
  }
}

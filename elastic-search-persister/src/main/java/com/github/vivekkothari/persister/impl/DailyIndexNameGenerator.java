package com.github.vivekkothari.persister.impl;

import static com.github.vivekkothari.river.service.IEnricher.df;

import com.github.vivekkothari.persister.IndexNameGenerator;
import com.github.vivekkothari.river.bean.MessageValue;
import java.util.Date;

/**
 * @author vivek.kothari on 26/07/16.
 */
public class DailyIndexNameGenerator
    implements IndexNameGenerator {

  @Override
  public String indexName(final MessageValue messageValue, final Date date) {
    return String.join("-", messageValue.getDatabase(), messageValue.getTable(), df.get()
        .format(date))
        .toLowerCase();
  }

}

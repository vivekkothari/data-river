package com.github.vivekkothari.persister;

import com.github.vivekkothari.river.bean.MessageValue;
import java.util.Date;

/**
 * @author vivek.kothari on 26/07/16.
 */
public interface IndexNameGenerator {

  String indexName(final MessageValue messageValue, final Date date);

}

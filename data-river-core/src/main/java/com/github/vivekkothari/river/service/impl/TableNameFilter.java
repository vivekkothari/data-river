package com.github.vivekkothari.river.service.impl;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IFilter;
import java.util.Objects;

/**
 * @author vivek.kothari on 30/05/16.
 */
public class TableNameFilter
    implements IFilter {

  private final String tableName;

  public TableNameFilter(final String tableName) {
    this.tableName = Objects.requireNonNull(tableName, "'tableName' cannot be null");
  }

  @Override
  public boolean shouldProcess(final MessageValue messageValue) {
    return tableName.equalsIgnoreCase(messageValue.getTable());
  }
}

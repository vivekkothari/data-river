package com.github.vivekkothari.river.service;

import com.github.vivekkothari.river.bean.MessageValue;

/**
 * @author vivek.kothari on 25/07/16.
 */
public interface IPersister {

  void persist(final MessageValue messageValue, final String riverType);
}

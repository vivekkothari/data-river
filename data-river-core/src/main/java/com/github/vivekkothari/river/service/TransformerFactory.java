package com.github.vivekkothari.river.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Singular;

/**
 * @author vivek.kothari on 30/05/16.
 */
public class TransformerFactory {

  public static TransformerFactory INSTANCE;

  private final Map<String, IEnricher> enrichers;

  private final Map<String, IFilter> filters;

  private final Map<String, IBackFiller> backFillers;

  private TransformerFactory(final Map<String, IEnricher> enrichers,
      final Map<String, IFilter> filters, final Map<String, IBackFiller> backFillers) {
    this.enrichers = enrichers;
    this.filters = filters;
    this.backFillers = backFillers;
    Preconditions
        .checkState(enrichers.size() == filters.size() && filters.size() == backFillers.size(),
            "There should be same number enrichers, filters and backFillers");
    for (final var entry : enrichers.entrySet()) {
      Preconditions.checkState(filters.containsKey(entry.getKey()));
      Preconditions.checkState(backFillers.containsKey(entry.getKey()));
    }
  }

  @Builder
  public static TransformerFactory transformerFactory(
      @Singular final Map<String, IEnricher> enrichers,
      @Singular final Map<String, IFilter> filters,
      @Singular final Map<String, IBackFiller> backFillers) {
    if (INSTANCE == null) {
      INSTANCE = new TransformerFactory(enrichers, filters, backFillers);
    }
    return INSTANCE;
  }

  public IEnricher enricher(final String riverType) {
    return enrichers.get(riverType);
  }

  public IFilter filter(final String riverType) {
    return filters.get(riverType);
  }

  public IBackFiller backFiller(final String riverType) {
    return backFillers.get(riverType);
  }

  public List<String> getRiverTypes() {
    return ImmutableList.copyOf(enrichers.keySet());
  }

}

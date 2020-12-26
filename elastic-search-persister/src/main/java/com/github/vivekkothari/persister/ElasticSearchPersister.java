package com.github.vivekkothari.persister;

import com.github.vivekkothari.river.bean.MessageValue;
import com.github.vivekkothari.river.service.IPersister;
import com.github.vivekkothari.river.service.TransformerFactory;
import java.util.Map;
import java.util.function.Supplier;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;

/**
 * @author vivek.kothari on 26/07/16.
 */
public class ElasticSearchPersister
    implements IPersister {

  private final Supplier<BulkProcessor> bulkProcessor;
  private final Map<String, IndexNameGenerator> indexNameGenerators;

  public ElasticSearchPersister(final Supplier<BulkProcessor> bulkProcessor,
      final Map<String, IndexNameGenerator> indexNameGenerators) {
    this.bulkProcessor = bulkProcessor;
    this.indexNameGenerators = indexNameGenerators;
  }

  @Override
  public void persist(final MessageValue messageValue, final String riverType) {
    final var enricher = TransformerFactory.INSTANCE.enricher(riverType);
    final String id = enricher.recordId(messageValue);
    final var indexName = indexNameGenerators.get(riverType)
        .indexName(messageValue, enricher.getRecordCreationDate(messageValue));
    switch (messageValue.getType()) {
      case insert, update -> bulkProcessor.get()
          .add(new IndexRequest(indexName).source(messageValue.getData()));
      case delete -> bulkProcessor.get().add(new DeleteRequest(indexName, id));
    }
  }

}
